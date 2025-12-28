use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;

#[derive(Clone)]
pub struct MemcachedDistributedCache {
    pub client: memcache::Client,
}

impl MemcachedDistributedCache {
    pub fn new(memcached_url: &str) -> anyhow::Result<Self> {
        let client = memcache::Client::connect(memcached_url)?;

        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl crate::DistributedCache for MemcachedDistributedCache {
    async fn cache_bytes(&self, key: &str, item: &[u8]) -> anyhow::Result<()> {
        let new_self = self.clone();
        let key_cloned = key.to_string();
        let item_cloned = item.to_vec();

        tokio::task::spawn_blocking(move || {
            let _ = new_self.client.set(&key_cloned, item_cloned.as_slice(), 0);
        })
        .await?;

        Ok(())
    }

    async fn retrieve_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
        let arc_self = Arc::new(self.clone());
        let key_cloned = key.to_string();

        let fut = tokio::task::spawn_blocking(move || -> anyhow::Result<Bytes> {
            let data: Vec<u8> = arc_self
                .client
                .get(&key_cloned)?
                .ok_or_else(|| anyhow::anyhow!("MemcachedDistributedCache: key not found"))?;

            Ok(Bytes::from(data))
        });

        fut.await?
    }
}

#[async_trait::async_trait]
impl crate::BatchingDistributedCache for MemcachedDistributedCache {
    async fn cache_batch<'a, I>(&self, items: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, Bytes)> + Send,
    {
        let arc_self = Arc::new(self.clone());
        let items_cloned: Vec<(String, Vec<u8>)> = items
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect();

        tokio::task::spawn_blocking(move || {
            for (k, v) in items_cloned {
                let _ = arc_self.client.set(&k, &*v, 0);
            }
        })
        .await?;
        Ok(())
    }

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<(&'a str, Bytes)>>
    where
        I: IntoIterator<Item = &'a str> + Send + Clone,
    {
        let new_self = self.clone();
        let keys_cloned = keys
            .clone()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<String>>();

        let fut =
            tokio::task::spawn_blocking(move || -> anyhow::Result<HashMap<String, Vec<u8>>> {
                let keys_slice = keys_cloned.iter().map(String::as_str).collect::<Vec<_>>();

                let map: HashMap<String, Vec<u8>> = new_self.client.gets(&keys_slice)?;

                Ok(map)
            });

        let map = fut.await??;

        Ok(keys
            .into_iter()
            .filter_map(|k| map.get(k).map(|v| (k, Bytes::from(v.clone()))))
            .collect::<Vec<(&'a str, Bytes)>>())
    }
}
