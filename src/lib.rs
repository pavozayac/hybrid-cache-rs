use crate::cache_entry::KeyValuePair;

mod cache_entry;
mod hybrid_cache;

#[async_trait::async_trait]
pub trait DistributedCache {
    async fn cache<V, I>(&self, key: I, item: V) -> anyhow::Result<()>
    where
        V: serde::Serialize + Send,
        I: Into<String> + Send + Clone;

    async fn retrieve<V, I>(&self, key: I) -> anyhow::Result<KeyValuePair<V>>
    where
        V: Send + serde::Serialize + for<'de> serde::Deserialize<'de>,
        I: Into<String> + Send + Clone;
}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}
