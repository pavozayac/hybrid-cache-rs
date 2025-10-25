use futures::future::TryJoinAll;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait DistributedCache {}

pub trait IntoKey: Send + Sync {
    fn into_key(self) -> String;
}

impl<T> IntoKey for T
where
    T: Into<String> + Send + Sync,
{
    fn into_key(self) -> String {
        self.into()
    }
}

pub trait Cacheable: Send + Sync {
    fn serialize(self, representation: CachedRepresentation) -> impl Iterator<Item = u8>;
}

pub trait Retrievable: Send + Sync {
    fn deserialize(payload: &[u8], representation: CachedRepresentation) -> Self;
}

pub trait CacheEntry: Cacheable + Retrievable {}

impl<T> CacheEntry for T where T: Cacheable + Retrievable {}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}

#[derive(Debug)]
pub struct KeyValuePair<K: IntoKey, V: CacheEntry> {
    pub key: K,
    pub value: V,
}

#[derive(Debug, Clone)]
pub struct HybridCache<T: DistributedCache> {
    in_memory_cache: Arc<moka::future::Cache<String, String>>,
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
        let memory_cache = moka::future::Cache::<String, String>::new(10_000);
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

impl<C, F> Cacheable for F
where
    C: Cacheable,
    F: Fn() -> C + Send + Sync,
{
    fn serialize(self, representation: CachedRepresentation) -> impl Iterator<Item = u8> {
        let item = self();
        item.serialize(representation)
    }
}

impl<T> HybridCache<T>
where
    T: DistributedCache + Send + Sync,
{
    pub async fn cache<C>(&self, key: impl IntoKey, item: C) -> anyhow::Result<()>
    where
        C: Cacheable,
    {
        self.in_memory_cache
            .insert(
                key.into_key(),
                item.serialize(CachedRepresentation::Binary)
                    .map(|c| c as char)
                    .collect(),
            )
            .await;

        Ok(())
    }

    pub async fn retrieve<K, V>(&self, key: K) -> anyhow::Result<KeyValuePair<K, V>>
    where
        K: IntoKey + Clone,
        V: CacheEntry,
    {
        let str_key = key.clone().into_key();

        let cached_bytes = self
            .in_memory_cache
            .get(&str_key)
            .await
            .ok_or(anyhow::anyhow!(
                "Could not retrieve value from memory cache"
            ))?;

        let kvp = KeyValuePair {
            key,
            value: V::deserialize(cached_bytes.as_bytes(), CachedRepresentation::Binary),
        };

        Ok(kvp)
    }

    pub async fn cache_many(
        &self,
        kvps: impl IntoIterator<Item = KeyValuePair<impl IntoKey, impl CacheEntry>>,
    ) {
        let _ = kvps
            .into_iter()
            .map(|kvp| self.cache(kvp.key, kvp.value))
            .collect::<TryJoinAll<_>>()
            .await;
    }

    pub async fn retrieve_many<K, V>(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> anyhow::Result<impl IntoIterator<Item = KeyValuePair<K, V>>>
    where
        K: IntoKey + Clone + 'static,
        V: CacheEntry + 'static,
    {
        let kvps = keys
            .into_iter()
            .map(|key| self.retrieve(key))
            .collect::<TryJoinAll<_>>()
            .await?
            .into_iter();

        Ok(kvps)
    }

    pub async fn retrieve_or_cache<K, V, F>(
        &self,
        key: K,
        constructor: F,
    ) -> anyhow::Result<KeyValuePair<K, V>>
    where
        K: IntoKey + Clone,
        V: CacheEntry + Clone,
        F: Fn(K) -> V,
    {
        if let Ok(value) = self.retrieve(key.clone()).await {
            return Ok(value);
        }

        let created = constructor(key.clone());
        self.cache(key.clone(), created.clone()).await?;

        Ok(KeyValuePair {
            key,
            value: created,
        })
    }

    pub async fn retrieve_or_cache_many<K, V, F>(
        &self,
        kvps: impl IntoIterator<Item = KeyValuePair<K, V>>,
        constructor: F,
    ) -> anyhow::Result<impl IntoIterator<Item = KeyValuePair<K, V>>>
    where
        K: IntoKey + Clone,
        V: CacheEntry + Clone,
        F: Fn(K) -> V + Copy,
    {
        let results = kvps
            .into_iter()
            .map(|kvp| self.retrieve_or_cache(kvp.key, constructor))
            .collect::<TryJoinAll<_>>()
            .await?
            .into_iter();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Debug, Clone)]
    struct TestDistributedCache;

    #[async_trait::async_trait]
    impl DistributedCache for TestDistributedCache {}

    #[derive(Debug, Clone, PartialEq)]
    struct TestData {
        value: String,
    }

    impl Cacheable for TestData {
        fn serialize(self, _representation: CachedRepresentation) -> impl Iterator<Item = u8> {
            self.value.into_bytes().into_iter()
        }
    }

    impl Retrievable for TestData {
        fn deserialize(payload: &[u8], _representation: CachedRepresentation) -> Self {
            TestData {
                value: String::from_utf8_lossy(payload).to_string(),
            }
        }
    }

    #[tokio::test]
    async fn test_hybrid_cache_new() {
        let distributed_cache = TestDistributedCache;
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
        let distributed_cache = TestDistributedCache;
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
        let distributed_cache = TestDistributedCache;
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Try to retrieve a key that doesn't exist
        let result: anyhow::Result<KeyValuePair<&str, TestData>> =
            cache.retrieve("nonexistent_key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_different_representations() {
        let distributed_cache = TestDistributedCache;

        // Test with Binary representation
        let mut cache_binary = HybridCache::builder()
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
        let distributed_cache = TestDistributedCache;
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
        let distributed_cache = TestDistributedCache;
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            KeyValuePair {
                key: "key1",
                value: TestData {
                    value: "value1".to_string(),
                },
            },
            KeyValuePair {
                key: "key2",
                value: TestData {
                    value: "value2".to_string(),
                },
            },
            KeyValuePair {
                key: "key3",
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
        let distributed_cache = TestDistributedCache;
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
        let results: Vec<KeyValuePair<&str, TestData>> = static_cache
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
        let distributed_cache = TestDistributedCache;
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
        let result: anyhow::Result<Vec<KeyValuePair<&str, TestData>>> = static_cache
            .retrieve_many(keys)
            .await
            .map(|iter| iter.into_iter().collect());

        // Should fail because some keys are missing
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_many_with_string_keys() {
        let distributed_cache = TestDistributedCache;
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
        let distributed_cache = TestDistributedCache;
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
            .retrieve_or_cache("existing_key", constructor)
            .await
            .unwrap();

        assert_eq!(result.key, "existing_key");
        assert_eq!(result.value.value, "existing_data");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_missing_key() {
        let distributed_cache = TestDistributedCache;
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
            .retrieve_or_cache("missing_key", constructor)
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
        let distributed_cache = TestDistributedCache;
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let constructor = |key: String| TestData {
            value: format!("constructed_{}", key),
        };

        // Test with owned String key
        let result = cache
            .retrieve_or_cache("string_key".to_string(), constructor)
            .await
            .unwrap();

        assert_eq!(result.key, "string_key");
        assert_eq!(result.value.value, "constructed_string_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_all_existing() {
        let distributed_cache = TestDistributedCache;
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
        let kvps = vec![
            KeyValuePair {
                key: "key1",
                value: TestData {
                    value: "dummy1".to_string(),
                },
            },
            KeyValuePair {
                key: "key2",
                value: TestData {
                    value: "dummy2".to_string(),
                },
            },
        ];

        let constructor = |_key: &str| TestData {
            value: "should_not_be_created".to_string(),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<&str, TestData>> = static_cache
            .retrieve_or_cache_many(kvps, constructor)
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
        let distributed_cache = TestDistributedCache;
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Create KVPs for non-existent keys
        let kvps = vec![
            KeyValuePair {
                key: "new_key1",
                value: TestData {
                    value: "dummy1".to_string(),
                },
            },
            KeyValuePair {
                key: "new_key2",
                value: TestData {
                    value: "dummy2".to_string(),
                },
            },
        ];

        let constructor = |key: &str| TestData {
            value: format!("constructed_{}", key),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<&str, TestData>> = static_cache
            .retrieve_or_cache_many(kvps, constructor)
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
        let distributed_cache = TestDistributedCache;
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
        let kvps = vec![
            KeyValuePair {
                key: "existing_key",
                value: TestData {
                    value: "dummy_existing".to_string(),
                },
            },
            KeyValuePair {
                key: "new_key",
                value: TestData {
                    value: "dummy_new".to_string(),
                },
            },
        ];

        let constructor = |key: &str| TestData {
            value: format!("constructed_{}", key),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<&str, TestData>> = static_cache
            .retrieve_or_cache_many(kvps, constructor)
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
        let distributed_cache = TestDistributedCache;
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            KeyValuePair {
                key: "string_key1".to_string(),
                value: TestData {
                    value: "dummy1".to_string(),
                },
            },
            KeyValuePair {
                key: "string_key2".to_string(),
                value: TestData {
                    value: "dummy2".to_string(),
                },
            },
        ];

        let constructor = |key: String| TestData {
            value: format!("string_constructed_{}", key),
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<KeyValuePair<String, TestData>> = static_cache
            .retrieve_or_cache_many(kvps, constructor)
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
}
