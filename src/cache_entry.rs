use serde_derive::{Deserialize, Serialize};

struct CacheEntry<V>
where
    V: serde::Serialize,
{
    key: String,
    payload: CacheEntryPayload<V>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CacheEntryPayload<V: serde::Serialize> {
    value: V,
    expiration_time: u64,
}

#[derive(Debug, Clone)]
pub struct KeyValuePair<V: serde::Serialize> {
    pub key: String,
    pub value: V,
}
