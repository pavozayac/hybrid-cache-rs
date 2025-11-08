use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CacheEntryPayload<V: serde::Serialize> {
    value: V,
    expiration_time: u64,
}
