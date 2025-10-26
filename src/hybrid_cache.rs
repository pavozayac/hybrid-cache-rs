use std::sync::Arc;

use anyhow::Context;
use futures::future::TryJoinAll;

use crate::{cache_entry::KeyValuePair, CachedRepresentation, DistributedCache};

type InMemoryCache = moka::future::Cache<String, Vec<u8>>;

#[derive(Debug, Clone)]
pub struct HybridCache<T: DistributedCache> {
    in_memory_cache: Arc<InMemoryCache>,
    distributed_cache: Arc<T>,
    in_memory_ttl: std::time::Duration,
    distributed_ttl: std::time::Duration,
    cached_representation: CachedRepresentation,
}

#[bon::bon]
impl<T: DistributedCache> HybridCache<T> {
    #[builder]
    pub fn new(
        distributed_cache: T,
        in_memory_ttl: Option<std::time::Duration>,
        distributed_ttl: Option<std::time::Duration>,
        cached_representation: CachedRepresentation,
    ) -> Self {
        let memory_cache: InMemoryCache = moka::future::Cache::new(10_000);
        let in_memory_ttl = in_memory_ttl.unwrap_or(std::time::Duration::from_secs(300));
        let distributed_ttl = distributed_ttl.unwrap_or(std::time::Duration::from_secs(300));

        HybridCache {
            in_memory_cache: Arc::new(memory_cache),
            distributed_cache: Arc::new(distributed_cache),
            in_memory_ttl,
            distributed_ttl,
            cached_representation,
        }
    }
}

fn serialize_repr(
    value: impl serde::Serialize,
    cached_representation: CachedRepresentation,
) -> anyhow::Result<Vec<u8>> {
    match cached_representation {
        CachedRepresentation::Binary => {
            postcard::to_allocvec(&value).context("Serialization failure using postcard")
        }
        CachedRepresentation::Json => {
            serde_json::to_vec(&value).context("Serialization failure using serde_json")
        }
    }
}

fn deserialize_repr<'de, V: serde::Deserialize<'de>>(
    bytes: &'de [u8],
    cached_representation: CachedRepresentation,
) -> anyhow::Result<V> {
    match cached_representation {
        CachedRepresentation::Binary => {
            postcard::from_bytes(bytes).context("Deserialization failure using postcard")
        }
        CachedRepresentation::Json => {
            serde_json::from_slice(bytes).context("Deserialization failure using serde_json")
        }
    }
}

impl<T> HybridCache<T>
where
    T: DistributedCache + Send + Sync,
{
    pub async fn cache<V, K>(&self, key: K, item: V) -> anyhow::Result<()>
    where
        K: Into<String> + Send + Clone,
        V: serde::Serialize + Send + Clone,
    {
        tokio::join!(
            self.cache_in_memory(key.clone(), item.clone()),
            self.distributed_cache.cache(key, item),
        );

        Ok(())
    }

    async fn cache_in_memory<V, K>(&self, key: K, item: V) -> Result<(), anyhow::Error>
    where
        K: Into<String> + Send + Clone,
        V: serde::Serialize + Send + Clone,
    {
        Ok(self
            .in_memory_cache
            .insert(
                key.clone().into(),
                serialize_repr(item.clone(), self.cached_representation)?,
            )
            .await)
    }

    pub async fn retrieve<V, I>(&self, key: I) -> anyhow::Result<KeyValuePair<V>>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Clone,
        I: Into<String> + Send + Clone,
    {
        let str_key = key.into();

        let in_memory_entry = self.retrieve_from_memory(str_key.clone()).await;

        match in_memory_entry {
            Ok(value) => Ok(value),
            Err(_) => {
                let distributed_entry: anyhow::Result<KeyValuePair<V>> =
                    self.retrieve_from_distributed(str_key).await;

                if let Ok(kvp) = distributed_entry {
                    self.cache_in_memory(kvp.key.clone(), kvp.value.clone())
                        .await;

                    Ok(KeyValuePair { ..kvp })
                } else {
                    distributed_entry
                }
            }
        }
    }

    async fn retrieve_from_memory<V>(
        &self,
        str_key: String,
    ) -> Result<KeyValuePair<V>, anyhow::Error>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let cached_bytes = self
            .in_memory_cache
            .get(&str_key)
            .await
            .ok_or(anyhow::anyhow!(
                "Could not retrieve value from memory cache"
            ))?;
        let kvp = KeyValuePair {
            key: str_key,
            value: deserialize_repr(cached_bytes.as_slice(), self.cached_representation)?,
        };
        Ok(kvp)
    }

    async fn retrieve_from_distributed<V>(
        &self,
        str_key: String,
    ) -> Result<KeyValuePair<V>, anyhow::Error>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
    {
        self.distributed_cache
            .retrieve(&str_key)
            .await
            .context("Could not retrieve value from distributed cache")
    }

    pub async fn cache_many<I, V>(&self, kvps: I)
    where
        V: serde::Serialize + Send + Clone,
        I: IntoIterator<Item = KeyValuePair<V>>,
    {
        let _ = kvps
            .into_iter()
            .map(|kvp| self.cache(kvp.key, kvp.value))
            .collect::<TryJoinAll<_>>()
            .await;
    }

    pub async fn retrieve_many<K, V, I>(
        &self,
        keys: I,
    ) -> anyhow::Result<impl IntoIterator<Item = KeyValuePair<V>>>
    where
        K: Into<String> + Send + Clone,
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Clone,
        I: IntoIterator<Item = K>,
    {
        let kvps = keys
            .into_iter()
            .map(|key| self.retrieve(key))
            .collect::<TryJoinAll<_>>()
            .await?
            .into_iter();

        Ok(kvps)
    }

    pub async fn retrieve_or_else<K, V, F>(
        &self,
        key: K,
        constructor: F,
    ) -> anyhow::Result<KeyValuePair<V>>
    where
        K: Into<String> + Clone + Send,
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send,
        F: Fn(K) -> V,
    {
        if let Ok(value) = self.retrieve(key.clone()).await {
            return Ok(value);
        }

        let created = constructor(key.clone());
        self.cache(key.clone(), created.clone()).await?;

        Ok(KeyValuePair {
            key: key.into(),
            value: created,
        })
    }

    pub async fn retrieve_many_or_else<K, V, F>(
        &self,
        keys: impl IntoIterator<Item = K>,
        constructor: F,
    ) -> anyhow::Result<impl IntoIterator<Item = KeyValuePair<V>>>
    where
        K: Into<String> + Clone + Send,
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send,
        F: Fn(K) -> V + Copy,
    {
        let results = keys
            .into_iter()
            .map(|key| self.retrieve_or_else(key, constructor))
            .collect::<TryJoinAll<_>>()
            .await?
            .into_iter();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use serde_derive::{Deserialize, Serialize};

    use crate::{CachedRepresentation, DistributedCache};

    use super::*;
    use std::{collections::HashMap, time::Duration};

    #[derive(Clone)]
    struct TestDistributedCache {
        cache: moka::future::Cache<String, Vec<u8>>,
    }

    impl Default for TestDistributedCache {
        fn default() -> Self {
            Self {
                cache: moka::future::Cache::new(10_000),
            }
        }
    }

    #[async_trait::async_trait]
    impl DistributedCache for TestDistributedCache {
        async fn cache<V, I: Into<String> + Send>(&self, key: I, item: V) -> anyhow::Result<()>
        where
            V: serde::Serialize + Send,
        {
            self.cache
                .insert(key.into(), serde_json::to_vec(&item)?)
                .await;
            Ok(())
        }

        async fn retrieve<V, I>(&self, key: I) -> anyhow::Result<KeyValuePair<V>>
        where
            V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send,
            I: Into<String> + Send + Clone,
        {
            Ok(KeyValuePair {
                key: key.clone().into(),
                value: serde_json::from_slice(
                    self.cache
                        .get(&key.into())
                        .await
                        .ok_or(anyhow::anyhow!("Mock error"))?
                        .as_slice(),
                )?,
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        value: String,
    }

    #[tokio::test]
    async fn test_hybrid_cache_new() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .in_memory_ttl(Duration::from_secs(60))
            .distributed_ttl(Duration::from_secs(300))
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Test that the cache is created successfully
        assert_eq!(
            cache.cached_representation as u8,
            CachedRepresentation::Binary as u8
        );
    }

    #[tokio::test]
    async fn test_cache_and_retrieve() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "test_value".to_string(),
        };

        // Cache the data
        cache.cache("test_key", test_data.clone()).await.unwrap();

        // Retrieve the data
        let retrieved: TestData = cache.retrieve("test_key").await.unwrap().value;
        assert_eq!(retrieved.value, test_data.value);
    }

    #[tokio::test]
    async fn test_retrieve_nonexistent_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Try to retrieve a key that doesn't exist
        let result: anyhow::Result<KeyValuePair<TestData>> =
            cache.retrieve("nonexistent_key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_different_representations() {
        let distributed_cache = TestDistributedCache::default();

        // Test with Binary representation
        let cache_binary = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "binary_test".to_string(),
        };

        cache_binary
            .cache("binary_key", test_data.clone())
            .await
            .unwrap();
        let retrieved: TestData = cache_binary.retrieve("binary_key").await.unwrap().value;
        assert_eq!(retrieved.value, test_data.value);

        // Test with JSON representation
        let cache_json = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Json)
            .build();

        let test_data_json = TestData {
            value: "json_test".to_string(),
        };

        cache_json
            .cache("json_key", test_data_json.clone())
            .await
            .unwrap();
        let retrieved_json: TestData = cache_json.retrieve("json_key").await.unwrap().value;
        assert_eq!(retrieved_json.value, test_data_json.value);
    }

    #[tokio::test]
    async fn test_cache_with_string_key() {
        let distributed_cache = TestDistributedCache::default();
        let mut cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "string_key_test".to_string(),
        };

        // Test with &str key
        cache.cache("string_key", test_data.clone()).await.unwrap();
        let retrieved: TestData = cache.retrieve("string_key").await.unwrap().value;
        assert_eq!(retrieved.value, test_data.value);

        // Test with String key
        cache
            .cache("owned_string_key".to_string(), test_data.clone())
            .await
            .unwrap();
        let retrieved2: TestData = cache
            .retrieve("owned_string_key".to_string())
            .await
            .unwrap()
            .value;
        assert_eq!(retrieved2.value, test_data.value);
    }

    #[tokio::test]
    async fn test_cache_many() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            KeyValuePair {
                key: "key1".to_string(),
                value: TestData {
                    value: "value1".to_string(),
                },
            },
            KeyValuePair {
                key: "key2".to_string(),
                value: TestData {
                    value: "value2".to_string(),
                },
            },
            KeyValuePair {
                key: "key3".to_string(),
                value: TestData {
                    value: "value3".to_string(),
                },
            },
        ];

        // Cache multiple items
        let static_cache = Box::leak(Box::new(cache));
        static_cache.cache_many(kvps).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify all items were cached
        let retrieved1: TestData = static_cache.retrieve("key1").await.unwrap().value;
        let retrieved2: TestData = static_cache.retrieve("key2").await.unwrap().value;
        let retrieved3: TestData = static_cache.retrieve("key3").await.unwrap().value;

        assert_eq!(retrieved1.value, "value1");
        assert_eq!(retrieved2.value, "value2");
        assert_eq!(retrieved3.value, "value3");
    }

    #[tokio::test]
    async fn test_retrieve_many() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // First, cache some test data
        let test_data1 = TestData {
            value: "multi_value1".to_string(),
        };
        let test_data2 = TestData {
            value: "multi_value2".to_string(),
        };
        let test_data3 = TestData {
            value: "multi_value3".to_string(),
        };

        cache.cache("multi_key1", test_data1.clone()).await.unwrap();
        cache.cache("multi_key2", test_data2.clone()).await.unwrap();
        cache.cache("multi_key3", test_data3.clone()).await.unwrap();

        // Retrieve multiple items
        let static_cache = Box::leak(Box::new(cache));
        let keys = vec!["multi_key1", "multi_key2", "multi_key3"];
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many(keys)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 3);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(sorted_results[0].value.value, "multi_value1");
        assert_eq!(sorted_results[1].value.value, "multi_value2");
        assert_eq!(sorted_results[2].value.value, "multi_value3");
    }

    #[tokio::test]
    async fn test_retrieve_many_with_missing_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Only cache one item
        let test_data = TestData {
            value: "existing_value".to_string(),
        };
        cache.cache("existing_key", test_data).await.unwrap();

        // Try to retrieve multiple keys where some don't exist
        let static_cache = Box::leak(Box::new(cache));
        let keys = vec!["existing_key", "missing_key1", "missing_key2"];
        let result: anyhow::Result<Vec<KeyValuePair<TestData>>> = static_cache
            .retrieve_many(keys)
            .await
            .map(|iter| iter.into_iter().collect());

        // Should fail because some keys are missing
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_many_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            KeyValuePair {
                key: "string_key1".to_string(),
                value: TestData {
                    value: "string_value1".to_string(),
                },
            },
            KeyValuePair {
                key: "string_key2".to_string(),
                value: TestData {
                    value: "string_value2".to_string(),
                },
            },
        ];

        let static_cache = Box::leak(Box::new(cache));
        static_cache.cache_many(kvps).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify items were cached with string keys
        let retrieved1: TestData = static_cache
            .retrieve("string_key1".to_string())
            .await
            .unwrap()
            .value;
        let retrieved2: TestData = static_cache
            .retrieve("string_key2".to_string())
            .await
            .unwrap()
            .value;

        assert_eq!(retrieved1.value, "string_value1");
        assert_eq!(retrieved2.value, "string_value2");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_existing_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "existing_data".to_string(),
        };

        // Cache the data first
        cache
            .cache("existing_key", test_data.clone())
            .await
            .unwrap();

        // Create a constructor that should not be called
        let constructor = |_key: &str| TestData {
            value: "should_not_be_created".to_string(),
        };

        // Retrieve or cache should return the existing value
        let result = cache
            .retrieve_or_else("existing_key", constructor)
            .await
            .unwrap();

        assert_eq!(result.key, "existing_key");
        assert_eq!(result.value.value, "existing_data");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_missing_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Create a constructor that will be called for missing keys
        let constructor = |key: &str| TestData {
            value: format!("constructed_value_for_{}", key),
        };

        // Retrieve or cache should create and cache the value
        let result = cache
            .retrieve_or_else("missing_key", constructor)
            .await
            .unwrap();

        assert_eq!(result.key, "missing_key");
        assert_eq!(result.value.value, "constructed_value_for_missing_key");

        // Verify the value was actually cached
        let retrieved: TestData = cache.retrieve("missing_key").await.unwrap().value;
        assert_eq!(retrieved.value, "constructed_value_for_missing_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let constructor = |key: String| TestData {
            value: format!("constructed_{}", key),
        };

        // Test with owned String key
        let result = cache
            .retrieve_or_else("string_key".to_string(), constructor)
            .await
            .unwrap();

        assert_eq!(result.key, "string_key");
        assert_eq!(result.value.value, "constructed_string_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_all_existing() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Cache some test data first
        let test_data1 = TestData {
            value: "existing_value1".to_string(),
        };
        let test_data2 = TestData {
            value: "existing_value2".to_string(),
        };

        cache.cache("key1", test_data1.clone()).await.unwrap();
        cache.cache("key2", test_data2.clone()).await.unwrap();

        // Create KVPs with dummy values (should not be used)
        let kvps = vec!["key1".to_string(), "key2".to_string()];

        let constructor = |_key| TestData {
            value: "should_not_be_created".to_string(),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many_or_else(kvps, constructor)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(sorted_results[0].key, "key1");
        assert_eq!(sorted_results[0].value.value, "existing_value1");
        assert_eq!(sorted_results[1].key, "key2");
        assert_eq!(sorted_results[1].value.value, "existing_value2");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_all_missing() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Create KVPs for non-existent keys
        let kvps = vec!["new_key1".to_string(), "new_key2".to_string()];

        let constructor = |key| TestData {
            value: format!("constructed_{}", key),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many_or_else(kvps, constructor)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(sorted_results[0].key, "new_key1");
        assert_eq!(sorted_results[0].value.value, "constructed_new_key1");
        assert_eq!(sorted_results[1].key, "new_key2");
        assert_eq!(sorted_results[1].value.value, "constructed_new_key2");

        // Verify values were actually cached
        let cached1: TestData = static_cache.retrieve("new_key1").await.unwrap().value;
        let cached2: TestData = static_cache.retrieve("new_key2").await.unwrap().value;

        assert_eq!(cached1.value, "constructed_new_key1");
        assert_eq!(cached2.value, "constructed_new_key2");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_mixed() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Cache one item first
        let existing_data = TestData {
            value: "pre_existing".to_string(),
        };
        cache.cache("existing_key", existing_data).await.unwrap();

        // Create KVPs with mix of existing and new keys
        let kvps = vec!["existing_key".to_string(), "new_key".to_string()];

        let constructor = |key| TestData {
            value: format!("constructed_{key}"),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many_or_else(kvps, constructor)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.key.cmp(&b.key));

        // Existing key should return pre-existing value
        assert_eq!(sorted_results[0].key, "existing_key");
        assert_eq!(sorted_results[0].value.value, "pre_existing");

        // New key should return constructed value
        assert_eq!(sorted_results[1].key, "new_key");
        assert_eq!(sorted_results[1].value.value, "constructed_new_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let keys = vec!["string_key1".to_string(), "string_key2".to_string()];

        let constructor = |key: String| TestData {
            value: format!("string_constructed_{}", key),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many_or_else(keys, constructor)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.key.cmp(&b.key));

        assert_eq!(sorted_results[0].key, "string_key1");
        assert_eq!(
            sorted_results[0].value.value,
            "string_constructed_string_key1"
        );
        assert_eq!(sorted_results[1].key, "string_key2");
        assert_eq!(
            sorted_results[1].value.value,
            "string_constructed_string_key2"
        );
    }

    #[tokio::test]
    async fn test_cache_populates_both_levels() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "both_levels_test".to_string(),
        };

        // Cache the data
        cache.cache("test_key", test_data.clone()).await.unwrap();

        // Verify data is in memory cache
        let memory_result = cache
            .retrieve_from_memory::<TestData>("test_key".to_string())
            .await;
        assert!(memory_result.is_ok());
        assert_eq!(memory_result.unwrap().value.value, test_data.value);

        // Verify data is in distributed cache by bypassing memory cache
        let distributed_result = cache
            .retrieve_from_distributed::<TestData>("test_key".to_string())
            .await;
        assert!(distributed_result.is_ok());
        // Note: TestDistributedCache returns "mock_key" as the key, but the value should match
        assert_eq!(distributed_result.unwrap().value.value, test_data.value);
    }

    #[tokio::test]
    async fn test_retrieve_fallback_from_memory_to_distributed() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "fallback_test".to_string(),
        };

        // Manually populate only the distributed cache
        distributed_cache
            .cache("fallback_key", test_data.clone())
            .await
            .unwrap();

        // Verify memory cache doesn't have the value
        let memory_result = cache
            .retrieve_from_memory::<TestData>("fallback_key".to_string())
            .await;
        assert!(memory_result.is_err());

        // Retrieve should fallback to distributed cache
        let retrieved: TestData = cache.retrieve("fallback_key").await.unwrap().value;
        assert_eq!(retrieved.value, test_data.value);
    }

    #[tokio::test]
    async fn test_retrieve_memory_cache_hit_skips_distributed() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let memory_data = TestData {
            value: "memory_priority".to_string(),
        };
        let distributed_data = TestData {
            value: "distributed_should_not_be_used".to_string(),
        };

        // Populate both caches with different values
        distributed_cache
            .cache("priority_key", distributed_data)
            .await
            .unwrap();

        // Manually populate memory cache
        let serialized = serialize_repr(memory_data.clone(), cache.cached_representation).unwrap();
        cache
            .in_memory_cache
            .insert("priority_key".to_string(), serialized)
            .await;

        // Retrieve should return memory cache value (not distributed)
        let retrieved: TestData = cache.retrieve("priority_key").await.unwrap().value;
        assert_eq!(retrieved.value, "memory_priority");
    }

    #[tokio::test]
    async fn test_retrieve_both_caches_miss() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Try to retrieve a key that doesn't exist in either cache
        let result: anyhow::Result<KeyValuePair<TestData>> =
            cache.retrieve("nonexistent_key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_many_populates_both_levels() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            KeyValuePair {
                key: "multi_key1".to_string(),
                value: TestData {
                    value: "multi_value1".to_string(),
                },
            },
            KeyValuePair {
                key: "multi_key2".to_string(),
                value: TestData {
                    value: "multi_value2".to_string(),
                },
            },
        ];

        let static_cache = Box::leak(Box::new(cache));
        static_cache.cache_many(kvps.clone()).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify both items are in memory cache
        for kvp in &kvps {
            let memory_result = static_cache
                .retrieve_from_memory::<TestData>(kvp.key.clone())
                .await;
            assert!(memory_result.is_ok());
            assert_eq!(memory_result.unwrap().value.value, kvp.value.value);
        }

        // Verify both items are in distributed cache
        for kvp in &kvps {
            let distributed_result = static_cache
                .retrieve_from_distributed::<TestData>(kvp.key.clone())
                .await;
            assert!(distributed_result.is_ok());
            assert_eq!(distributed_result.unwrap().value.value, kvp.value.value);
        }
    }

    #[tokio::test]
    async fn test_retrieve_many_mixed_cache_levels() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let memory_only_data = TestData {
            value: "memory_only".to_string(),
        };
        let distributed_only_data = TestData {
            value: "distributed_only".to_string(),
        };
        let both_caches_data = TestData {
            value: "both_caches".to_string(),
        };

        // Populate memory only
        let serialized_memory =
            serialize_repr(memory_only_data.clone(), cache.cached_representation).unwrap();
        cache
            .in_memory_cache
            .insert("memory_key".to_string(), serialized_memory)
            .await;

        // Populate distributed only
        distributed_cache
            .cache("distributed_key", distributed_only_data.clone())
            .await
            .unwrap();

        // Populate both caches
        cache
            .cache("both_key", both_caches_data.clone())
            .await
            .unwrap();

        let static_cache = Box::leak(Box::new(cache));
        let keys = vec!["memory_key", "distributed_key", "both_key"];
        let results: Vec<KeyValuePair<TestData>> = static_cache
            .retrieve_many(keys)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(results.len(), 3);

        // Find results by key
        let memory_result = results.iter().find(|kvp| kvp.key == "memory_key").unwrap();
        let distributed_result = results
            .iter()
            .find(|kvp| kvp.key == "distributed_key")
            .unwrap();
        let both_result = results.iter().find(|kvp| kvp.key == "both_key").unwrap();

        assert_eq!(memory_result.value.value, "memory_only");
        assert_eq!(distributed_result.value.value, "distributed_only");
        assert_eq!(both_result.value.value, "both_caches");
    }

    #[tokio::test]
    async fn test_retrieve_or_else_populates_both_caches_on_miss() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let constructor = |key: &str| TestData {
            value: format!("constructed_{}", key),
        };

        // Key doesn't exist in either cache
        let result = cache
            .retrieve_or_else("new_key", constructor)
            .await
            .unwrap();
        assert_eq!(result.value.value, "constructed_new_key");

        // Verify both caches now have the constructed value
        let memory_result = cache
            .retrieve_from_memory::<TestData>("new_key".to_string())
            .await;
        assert!(memory_result.is_ok());
        assert_eq!(memory_result.unwrap().value.value, "constructed_new_key");

        let distributed_result = cache
            .retrieve_from_distributed::<TestData>("new_key".to_string())
            .await;
        assert!(distributed_result.is_ok());
        assert_eq!(
            distributed_result.unwrap().value.value,
            "constructed_new_key"
        );
    }

    #[tokio::test]
    async fn test_retrieve_or_else_fallback_behavior() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let distributed_data = TestData {
            value: "from_distributed".to_string(),
        };

        // Populate only distributed cache
        distributed_cache
            .cache("fallback_test_key", distributed_data.clone())
            .await
            .unwrap();

        let constructor = |_key: &str| TestData {
            value: "should_not_construct".to_string(),
        };

        // Should retrieve from distributed cache, not construct
        let result = cache
            .retrieve_or_else("fallback_test_key", constructor)
            .await
            .unwrap();
        assert_eq!(result.value.value, "from_distributed");
    }

    #[tokio::test]
    async fn test_cache_with_different_serialization_formats() {
        let distributed_cache = TestDistributedCache::default();

        // Test Binary serialization
        let binary_cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "serialization_test".to_string(),
        };

        binary_cache
            .cache("binary_key", test_data.clone())
            .await
            .unwrap();

        // Verify memory cache has binary serialized data
        let binary_bytes = binary_cache
            .in_memory_cache
            .get("binary_key")
            .await
            .unwrap();
        let deserialized_binary: TestData =
            deserialize_repr(&binary_bytes, CachedRepresentation::Binary).unwrap();
        assert_eq!(deserialized_binary.value, test_data.value);

        // Test JSON serialization
        let json_cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Json)
            .build();

        json_cache
            .cache("json_key", test_data.clone())
            .await
            .unwrap();

        // Verify memory cache has JSON serialized data
        let json_bytes = json_cache.in_memory_cache.get("json_key").await.unwrap();
        let deserialized_json: TestData =
            deserialize_repr(&json_bytes, CachedRepresentation::Json).unwrap();
        assert_eq!(deserialized_json.value, test_data.value);
    }
}
