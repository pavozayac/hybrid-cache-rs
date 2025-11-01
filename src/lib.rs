use bytes::Bytes;

mod cache_entry;
mod hybrid_cache;
pub mod redis_impl;
pub use redis_impl::RedisDistributedCache;

#[async_trait::async_trait]
pub trait DistributedCache {
    async fn cache_bytes(&self, key: &str, item: &[u8]) -> anyhow::Result<()>;

    async fn retrieve_bytes(&self, key: &str) -> anyhow::Result<Bytes>;
}

#[async_trait::async_trait]
pub trait BatchingDistributedCache {
    async fn cache_batch<'a, I>(&self, items: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, Bytes)> + Send;

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<Option<Bytes>>>
    where
        I: IntoIterator<Item = &'a str> + Send;
}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}
