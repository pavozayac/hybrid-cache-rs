use bytes::Bytes;

use crate::{BatchingDistributedCache, DistributedCache};

#[derive(Debug, Clone, Default)]
pub struct NoopDistributedCache;

impl NoopDistributedCache {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl DistributedCache for NoopDistributedCache {
    async fn cache_bytes(&self, _key: &str, _item: &[u8]) -> anyhow::Result<()> {
        // no-op: pretend to store
        Ok(())
    }

    async fn retrieve_bytes(&self, _key: &str) -> anyhow::Result<Bytes> {
        // no-op: indicate not found by returning an error similar to redis impl when missing
        Err(anyhow::anyhow!("NoopDistributedCache: key not found"))
    }
}

#[async_trait::async_trait]
impl BatchingDistributedCache for NoopDistributedCache {
    async fn cache_batch<'a, I>(&self, _items: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, Bytes)> + Send,
    {
        // no-op
        Ok(())
    }

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a str> + Send,
    {
        let keys_vec: Vec<&'a str> = keys.into_iter().collect();
        // Return a vector of None for all requested keys to indicate misses
        Ok(keys_vec.into_iter().map(|_| None).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cache_entry::KeyValuePair, CachedRepresentation, HybridCache};
    use rand::distr::Alphanumeric;
    use rand::Rng;

    #[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct TestData {
        value: String,
    }

    fn random_string(len: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn noop_hybrid_single_cache_and_retrieve() {
        let noop = NoopDistributedCache::new();
        let cache = HybridCache::builder()
            .distributed_cache(noop)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let data = TestData {
            value: "hello-noop".to_string(),
        };

        cache.cache("test_key", data.clone()).await.unwrap();

        let got: TestData = cache.retrieve("test_key").await.unwrap().value;
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn noop_hybrid_batch_1000_inserts() {
        let noop = NoopDistributedCache::new();
        let cache = HybridCache::builder()
            .distributed_cache(noop)
            .cached_representation(CachedRepresentation::Binary)
            .build();

        let size = 1000usize;
        let kvps: Vec<KeyValuePair<TestData>> = (0..size)
            .map(|_| KeyValuePair {
                key: random_string(16),
                value: TestData {
                    value: random_string(64),
                },
            })
            .collect();

        cache.clone().cache_many(kvps.clone()).await;

        let keys: Vec<String> = kvps.iter().map(|kv| kv.key.clone()).collect();
        let retrieved: Vec<KeyValuePair<TestData>> = cache
            .retrieve_many(keys)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(retrieved.len(), size);

        for i in 0..size {
            assert_eq!(retrieved[i].value, kvps[i].value);
        }
    }
}
