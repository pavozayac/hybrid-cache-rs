use crate::cache_entry::KeyValuePair;

mod cache_entry;
mod hybrid_cache;

#[async_trait::async_trait]
pub trait DistributedCache {
    async fn cache<V, I: Into<String> + Send>(&self, key: I, item: V) -> anyhow::Result<()>
    where
        V: serde::Serialize + Send;

    async fn retrieve<V, I: Into<String> + Send>(&self, key: I) -> anyhow::Result<KeyValuePair<V>>
    where
        V: Send + serde::Serialize + for<'de> serde::Deserialize<'de>;
}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}
