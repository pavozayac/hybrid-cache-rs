mod cache_entry;
mod hybrid_cache;

#[async_trait::async_trait]
pub trait DistributedCache {}

#[derive(Debug, Clone, Copy)]
pub enum CachedRepresentation {
    Binary,
    Json,
}
