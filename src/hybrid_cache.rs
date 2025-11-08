use std::sync::Arc;

use anyhow::Context;
use futures::future::JoinAll;

use crate::{CachedRepresentation, DistributedCache, entry::AsEntries, key::IntoKeys};

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
    async fn set_one<S: serde::Serialize + Send>(&self, key: &str, value: S) -> anyhow::Result<()> {
        let serialized_value = serialize_repr(&value, self.cached_representation)?;

        let ((), ()) = tokio::join!(
            self.cache_in_memory(key, &serialized_value),
            self.cache_in_distributed(key, &serialized_value)
        );

        Ok(())
    }

    async fn cache_in_memory(&self, key: &str, value: impl AsRef<[u8]>) {
        self.in_memory_cache
            .insert(key.to_string(), value.as_ref().to_vec())
            .await;
    }

    async fn cache_in_distributed(&self, key: &str, value: &[u8]) {
        self.distributed_cache.cache_bytes(key, value).await;
    }

    async fn get_one<V>(&self, key: &str) -> anyhow::Result<V>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Clone,
    {
        let in_memory_entry: anyhow::Result<_> = self.retrieve_from_memory(key).await;

        if let Ok(value) = &in_memory_entry {
            return deserialize_repr(value, self.cached_representation);
        }

        let distributed_entry: V = self.retrieve_from_distributed(key).await?;

        let () = self
            .cache_in_memory(
                key,
                serialize_repr(&distributed_entry, self.cached_representation)?,
            )
            .await;

        Ok(distributed_entry)
    }

    async fn retrieve_from_memory(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let cached_bytes = self.in_memory_cache.get(key).await.ok_or(anyhow::anyhow!(
            "Could not retrieve value from memory cache"
        ))?;

        Ok(cached_bytes)
    }

    async fn retrieve_from_distributed<V>(&self, key: &str) -> anyhow::Result<V>
    where
        V: for<'de> serde::Deserialize<'de> + Send,
    {
        let bytes = self
            .distributed_cache
            .retrieve_bytes(key)
            .await
            .context("Could not retrieve value from distributed cache")?;

        let output = deserialize_repr(bytes.as_ref(), self.cached_representation)?;

        Ok(output)
    }

    pub async fn set<S: serde::Serialize + Send + Sync>(&self, entries: impl AsEntries<S>) {
        let _ = entries
            .as_entries()
            .into_iter()
            .map(|(key, value)| self.set_one(key, value))
            .collect::<JoinAll<_>>()
            .await;
    }

    pub async fn get<'a, V>(&self, key: impl AsRef<str>) -> Option<V>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Clone,
    {
        let item = self.get_one::<V>(key.as_ref()).await;

        item.ok()
    }

    pub async fn get_many<'a, V, K: IntoKeys + 'a>(
        &self,
        keys: K,
    ) -> impl IntoIterator<Item = (<K as IntoKeys>::Key<'a>, V)>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Clone,
    {
        keys.into_keys()
            .into_iter()
            .map(|key| async move {
                let key_ref: &str = key.as_ref();
                let val = self.get_one(key_ref).await;

                (key, val)
            })
            .map(Box::pin)
            .collect::<JoinAll<_>>()
            .await
            .into_iter()
            .filter_map(|(key, value)| value.ok().map(|v| (key, v)))
    }

    async fn get_or_create_one<'a, V, F>(
        &self,
        key: &'a str,
        constructor: F,
    ) -> anyhow::Result<(&'a str, V)>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync,
        F: Fn(&'a str) -> V,
    {
        if let Ok(value) = self.get_one(key).await {
            return Ok((key, value));
        }

        let created = constructor(key);
        self.set_one(key, &created).await?;

        Ok((key, created))
    }

    pub async fn get_or_create<V>(
        &self,
        key: impl AsRef<str>,
        constructor: impl for<'a> AsyncFn(&'a str) -> Option<V>,
    ) -> Option<V>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync,
    {
        let key_str = key.as_ref();
        if let Ok(value) = self.get_one(key_str).await {
            return Some(value);
        }

        if let Some(created) = constructor(key_str).await {
            let _ = self.set_one(key_str, &created).await;
            Some(created)
        } else {
            None
        }
    }

    pub async fn get_many_or_create<'a, V, K: IntoKeys + 'a>(
        &self,
        keys: K,
        constructor: impl for<'b> AsyncFn(&'b str) -> Option<V>,
    ) -> impl IntoIterator<Item = (<K as IntoKeys>::Key<'a>, V)>
    where
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Send + Sync,
    {
        keys.into_keys()
            .into_iter()
            .map(|key| async {
                if let Ok(value) = self.get_one::<V>(key.as_ref()).await {
                    return Result::<(<K as IntoKeys>::Key<'a>, V), ()>::Ok((key, value));
                }

                let created = constructor(key.as_ref()).await.ok_or(())?;
                self.set_one(key.as_ref(), &created)
                    .await
                    .map_err(|_err| ())?;

                Ok((key, created))
            })
            .collect::<JoinAll<_>>()
            .await
            .into_iter()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_derive::{Deserialize, Serialize};

    use crate::{CachedRepresentation, DistributedCache};

    use super::*;
    use std::time::Duration;

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
        async fn cache_bytes(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            self.cache.insert(key.to_string(), bytes.to_vec()).await;
            Ok(())
        }

        async fn retrieve_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
            self.cache
                .get(key)
                .await
                .ok_or(anyhow::anyhow!("Mock error"))
                .map(Bytes::from)
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
        cache.set(("test_key", &test_data)).await;

        // Retrieve the data
        let item: TestData = cache.get(String::from("test_key")).await.unwrap();
        assert_eq!(item.value, test_data.value);
    }

    #[tokio::test]
    async fn test_retrieve_nonexistent_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // Try to retrieve a key that doesn't exist
        let result: Option<TestData> = cache.get("nonexistent_key").await;

        assert!(result.is_none());
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

        cache_binary.set(("binary_key", test_data.clone())).await;

        let retrieved: TestData = cache_binary.get("binary_key").await.unwrap();
        assert_eq!(retrieved.value, test_data.value);

        // Test with JSON representation
        let cache_json = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Json)
            .build();

        let test_data_json = TestData {
            value: "json_test".to_string(),
        };

        cache_json.set(("json_key", test_data_json.clone())).await;

        let retrieved_json: TestData = cache_json.get("json_key").await.unwrap();

        assert_eq!(retrieved_json.value, test_data_json.value);
    }

    #[tokio::test]
    async fn test_cache_with_string_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let test_data = TestData {
            value: "string_key_test".to_string(),
        };

        // Test with &str key
        cache.set(("string_key", test_data.clone())).await;

        let retrieved: TestData = cache.get("string_key").await.unwrap();
        assert_eq!(retrieved.value, test_data.value);

        // Test with String key
        cache
            .set(("owned_string_key".to_string(), test_data.clone()))
            .await;

        let retrieved2: TestData = cache.get("owned_string_key".to_string()).await.unwrap();

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
            (
                "key1".to_string(),
                TestData {
                    value: "value1".to_string(),
                },
            ),
            (
                "key2".to_string(),
                TestData {
                    value: "value2".to_string(),
                },
            ),
            (
                "key3".to_string(),
                TestData {
                    value: "value3".to_string(),
                },
            ),
        ];

        // Cache multiple items
        let static_cache = Box::leak(Box::new(cache));
        static_cache.set(kvps).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify all items were cached
        let retrieved1: TestData = static_cache.get("key1").await.unwrap();
        let retrieved2: TestData = static_cache.get("key2").await.unwrap();
        let retrieved3: TestData = static_cache.get("key3").await.unwrap();

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

        cache.set(("multi_key1", test_data1.clone())).await;
        cache.set(("multi_key2", test_data2.clone())).await;
        cache.set(("multi_key3", test_data3.clone())).await;

        // Retrieve multiple items
        let static_cache = Box::leak(Box::new(cache));
        let keys = vec!["multi_key1", "multi_key2", "multi_key3"];
        let results: Vec<(&str, TestData)> =
            static_cache.get_many(keys).await.into_iter().collect();

        assert_eq!(results.len(), 3);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sorted_results[0].1.value, "multi_value1");
        assert_eq!(sorted_results[1].1.value, "multi_value2");
        assert_eq!(sorted_results[2].1.value, "multi_value3");
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

        cache.set(("existing_key", test_data)).await;

        // Try to retrieve multiple keys where some don't exist
        let static_cache = Box::leak(Box::new(cache));
        let keys = vec!["existing_key", "missing_key1", "missing_key2"];
        let result: Vec<(&str, TestData)> = static_cache.get_many(keys).await.into_iter().collect();

        // Should only retrieve the present keys
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "existing_key");
        assert_eq!(result[0].1.value, "existing_value");
    }

    #[tokio::test]
    async fn test_cache_many_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            (
                "string_key1".to_string(),
                TestData {
                    value: "string_value1".to_string(),
                },
            ),
            (
                "string_key2".to_string(),
                TestData {
                    value: "string_value2".to_string(),
                },
            ),
        ];

        let static_cache = Box::leak(Box::new(cache));
        static_cache.set(kvps).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify items were cached with string keys
        let retrieved1: TestData = static_cache.get("string_key1").await.unwrap();

        let retrieved2: TestData = static_cache.get("string_key2").await.unwrap();

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
        cache.set(("existing_key", &test_data)).await;

        // Retrieve or cache should return the existing value
        let result: TestData = cache
            .get_or_create("existing_key", async |_key| {
                Some(TestData {
                    value: "should_not_be_created".to_string(),
                })
            })
            .await
            .unwrap();

        assert_eq!(result.value, "existing_data");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_missing_key() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        // // Create a constructor that will be called for missing keys
        // let constructor = ;

        // Retrieve or cache should create and cache the value
        let result = cache
            .get_or_create("missing_key", async |key| {
                Some(TestData {
                    value: format!("constructed_value_for_{}", key),
                })
            })
            .await
            .unwrap();

        assert_eq!(result.value, "constructed_value_for_missing_key");

        // Verify the value was actually cached
        let retrieved: TestData = cache.get("missing_key").await.unwrap();
        assert_eq!(retrieved.value, "constructed_value_for_missing_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let constructor = async |key: &str| {
            Some(TestData {
                value: format!("constructed_{}", key),
            })
        };

        // Test with owned String key
        let result = cache
            .get_or_create("string_key".to_string(), constructor)
            .await
            .unwrap();

        assert_eq!(result.value, "constructed_string_key");
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

        cache.set(("key1", test_data1.clone())).await;
        cache.set(("key2", test_data2.clone())).await;

        // Create KVPs with dummy values (should not be used)
        let kvps = vec!["key1".to_string(), "key2".to_string()];

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<(String, TestData)> = static_cache
            .get_many_or_create(kvps, async |_key| {
                Some(TestData {
                    value: "should_not_be_created".to_string(),
                })
            })
            .await
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sorted_results[0].0, "key1");
        assert_eq!(sorted_results[0].1.value, "existing_value1");
        assert_eq!(sorted_results[1].0, "key2");
        assert_eq!(sorted_results[1].1.value, "existing_value2");
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

        let constructor = async |key: &str| {
            Some(TestData {
                value: format!("constructed_{}", key),
            })
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<(String, TestData)> = static_cache
            .get_many_or_create(kvps, constructor)
            .await
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sorted_results[0].0, "new_key1");
        assert_eq!(sorted_results[0].1.value, "constructed_new_key1");
        assert_eq!(sorted_results[1].0, "new_key2");
        assert_eq!(sorted_results[1].1.value, "constructed_new_key2");

        // Verify values were actually cached
        let cached1: TestData = static_cache.get("new_key1").await.unwrap();
        let cached2: TestData = static_cache.get("new_key2").await.unwrap();

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
        cache.set(("existing_key", existing_data)).await;

        // Create KVPs with mix of existing and new keys
        let kvps = vec!["existing_key".to_string(), "new_key".to_string()];

        let constructor = async |key: &str| {
            Some(TestData {
                value: format!("constructed_{key}"),
            })
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<(String, TestData)> = static_cache
            .get_many_or_create(kvps, constructor)
            .await
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        // Sort results by key for consistent testing
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Existing key should return pre-existing value
        assert_eq!(sorted_results[0].0, "existing_key");
        assert_eq!(sorted_results[0].1.value, "pre_existing");

        // New key should return constructed value
        assert_eq!(sorted_results[1].0, "new_key");
        assert_eq!(sorted_results[1].1.value, "constructed_new_key");
    }

    #[tokio::test]
    async fn test_retrieve_or_cache_many_with_string_keys() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let keys = vec!["string_key1".to_string(), "string_key2".to_string()];

        let constructor = async |key: &str| {
            Some(TestData {
                value: format!("string_constructed_{}", key),
            })
        };

        let static_cache = Box::leak(Box::new(cache));
        let results: Vec<(String, TestData)> = static_cache
            .get_many_or_create(keys, constructor)
            .await
            .into_iter()
            .collect();

        assert_eq!(results.len(), 2);

        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sorted_results[0].0, "string_key1");
        assert_eq!(sorted_results[0].1.value, "string_constructed_string_key1");
        assert_eq!(sorted_results[1].0, "string_key2");
        assert_eq!(sorted_results[1].1.value, "string_constructed_string_key2");
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
        cache.set(("test_key", test_data.clone())).await;

        // Verify data is in memory cache
        let memory_result = cache.retrieve_from_memory("test_key").await;
        assert!(memory_result.is_ok());

        let memory_value: TestData =
            deserialize_repr(&memory_result.unwrap(), cache.cached_representation).unwrap();
        assert_eq!(memory_value, test_data);

        // Verify data is in distributed cache by bypassing memory cache
        let distributed_result = cache
            .retrieve_from_distributed::<TestData>("test_key")
            .await;
        assert!(distributed_result.is_ok());
        // Note: TestDistributedCache returns "mock_key" as the key, but the value should match
        assert_eq!(distributed_result.unwrap().value, test_data.value);
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
            .cache_bytes(
                "fallback_key",
                &serialize_repr(test_data.clone(), cache.cached_representation).unwrap(),
            )
            .await
            .unwrap();

        // Verify memory cache doesn't have the value
        let memory_result = cache.retrieve_from_memory("fallback_key").await;
        assert!(memory_result.is_err());

        // Retrieve should fallback to distributed cache
        let retrieved: TestData = cache.get("fallback_key").await.unwrap();
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
            .cache_bytes(
                "priority_key",
                &serialize_repr(distributed_data, CachedRepresentation::Binary).unwrap(),
            )
            .await
            .unwrap();

        // Manually populate memory cache
        let serialized = serialize_repr(memory_data.clone(), cache.cached_representation).unwrap();
        cache
            .in_memory_cache
            .insert("priority_key".to_string(), serialized)
            .await;

        // Retrieve should return memory cache value (not distributed)
        let retrieved: TestData = cache.get("priority_key").await.unwrap();
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
        let result: Option<TestData> = cache.get("nonexistent_key").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cache_many_populates_both_levels() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let kvps = vec![
            (
                "multi_key1".to_string(),
                TestData {
                    value: "multi_value1".to_string(),
                },
            ),
            (
                "multi_key2".to_string(),
                TestData {
                    value: "multi_value2".to_string(),
                },
            ),
        ];

        let static_cache = Box::leak(Box::new(cache));
        static_cache.set(kvps.clone()).await;

        // Give tasks time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify both items are in memory cache
        for kvp in &kvps {
            let memory_result = static_cache.retrieve_from_memory(&kvp.0).await;

            assert!(memory_result.is_ok());

            let parsed = postcard::from_bytes::<TestData>(&memory_result.unwrap());

            assert_eq!(parsed.unwrap().value, kvp.1.value);
        }
        // Verify both items are in memory cache
        for (key, value) in &kvps {
            let memory_result = static_cache.retrieve_from_memory(key).await;
            assert!(memory_result.is_ok());
            let memory_value: TestData =
                deserialize_repr(&memory_result.unwrap(), static_cache.cached_representation)
                    .unwrap();
            assert_eq!(memory_value.value, value.value);
        }

        // Verify both items are in distributed cache
        for (key, value) in &kvps {
            let distributed_result = static_cache
                .retrieve_from_distributed::<TestData>(key)
                .await;
            assert!(distributed_result.is_ok());
            assert_eq!(distributed_result.unwrap().value, value.value);
        }

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
            serialize_repr(memory_only_data.clone(), static_cache.cached_representation).unwrap();

        static_cache
            .in_memory_cache
            .insert("memory_key".to_string(), serialized_memory)
            .await;

        // Populate distributed only
        distributed_cache
            .cache_bytes(
                "distributed_key",
                &serialize_repr(distributed_only_data.clone(), CachedRepresentation::Binary)
                    .unwrap(),
            )
            .await
            .unwrap();

        // Populate both caches
        static_cache
            .set(("both_key", both_caches_data.clone()))
            .await;

        let keys = vec!["memory_key", "distributed_key", "both_key"];
        let results: Vec<(&str, TestData)> =
            static_cache.get_many(keys).await.into_iter().collect();

        assert_eq!(results.len(), 3);

        // Find results by key
        let memory_result = results.iter().find(|&kvp| kvp.0 == "memory_key").unwrap();
        let distributed_result = results
            .iter()
            .find(|kvp| kvp.0 == "distributed_key")
            .unwrap();
        let both_result = results.iter().find(|kvp| kvp.0 == "both_key").unwrap();

        assert_eq!(memory_result.1.value, "memory_only");
        assert_eq!(distributed_result.1.value, "distributed_only");
        assert_eq!(both_result.1.value, "both_caches");
    }

    #[tokio::test]
    async fn test_retrieve_or_else_populates_both_caches_on_miss() {
        let distributed_cache = TestDistributedCache::default();
        let cache = HybridCache::builder()
            .distributed_cache(distributed_cache.clone())
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let constructor = async |key: &str| {
            Some(TestData {
                value: format!("constructed_{}", key),
            })
        };

        // Key doesn't exist in either cache
        let result = cache.get_or_create("new_key", constructor).await.unwrap();

        assert_eq!(result.value, "constructed_new_key");

        // Verify both caches now have the constructed value
        let memory_result = cache.retrieve_from_memory("new_key").await;

        assert!(memory_result.is_ok());

        let parsed = postcard::from_bytes::<TestData>(&memory_result.unwrap()).unwrap();

        assert_eq!(parsed.value, "constructed_new_key");

        let distributed_result = cache.retrieve_from_distributed::<TestData>("new_key").await;

        assert!(distributed_result.is_ok());
        assert_eq!(distributed_result.unwrap().value, "constructed_new_key");
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
            .cache_bytes(
                "fallback_test_key",
                &serialize_repr(distributed_data.clone(), CachedRepresentation::Binary).unwrap(),
            )
            .await
            .unwrap();

        let constructor = async |_key: &str| {
            Some(TestData {
                value: "should_not_construct".to_string(),
            })
        };

        // Should retrieve from distributed cache, not construct
        let result = cache
            .get_or_create("fallback_test_key", constructor)
            .await
            .unwrap();

        assert_eq!(result.value, "from_distributed");
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

        binary_cache.set(("binary_key", test_data.clone())).await;

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

        json_cache.set(("json_key", test_data.clone())).await;

        // Verify memory cache has JSON serialized data
        let json_bytes = json_cache.in_memory_cache.get("json_key").await.unwrap();
        let deserialized_json: TestData =
            deserialize_repr(&json_bytes, CachedRepresentation::Json).unwrap();
        assert_eq!(deserialized_json.value, test_data.value);
    }
}
