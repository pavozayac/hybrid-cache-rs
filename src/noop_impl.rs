use bytes::Bytes;

use crate::{BatchingDistributedCache, DistributedCache};

#[derive(Debug, Clone, Default)]
pub struct NoopDistributedCache;

impl NoopDistributedCache {
    #[must_use]
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

    async fn retrieve_batch<'a, I>(&self, _keys: I) -> anyhow::Result<Vec<(&'a str, Bytes)>>
    where
        I: IntoIterator<Item = &'a str> + Send,
    {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CachedRepresentation, HybridCache};
    use rand::Rng;
    use rand::distr::Alphanumeric;

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

        cache.set_many(("test_key", data.clone())).await;

        let got: TestData = cache.get("test_key").await.unwrap();
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
        let kvps: Vec<_> = (0..size)
            .map(|_| {
                (
                    random_string(16),
                    TestData {
                        value: random_string(64),
                    },
                )
            })
            .collect();

        cache.clone().set_many(kvps.clone()).await;

        let keys: Vec<String> = kvps.iter().map(|kv| kv.0.clone()).collect();
        let retrieved: Vec<(String, TestData)> = cache.get_many(keys).await.into_iter().collect();

        assert_eq!(retrieved.len(), size);

        for i in 0..size {
            assert_eq!(retrieved[i].1, kvps[i].1);
        }
    }
}
