use bytes::Bytes;
pub mod backends;
mod hybrid_cache;
pub use hybrid_cache::HybridCache;
mod cache_entry;
mod entry;
mod key;

pub use backends::memcached_impl::MemcachedDistributedCache;
pub use backends::noop_impl::NoopDistributedCache;
pub use backends::redis_impl::RedisDistributedCache;

#[cfg_attr(test, mockall::automock)]
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

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<(&'a str, Bytes)>>
    where
        I: IntoIterator<Item = &'a str> + Send + Clone;
}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}
