use bytes::Bytes;
use redis::AsyncCommands;

use crate::DistributedCache;

#[derive(Debug, Clone)]
pub struct RedisDistributedCache {
    pub client: redis::Client,
}

impl RedisDistributedCache {
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl DistributedCache for RedisDistributedCache {
    async fn cache_bytes(&self, key: &str, item: &[u8]) -> anyhow::Result<()> {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;

        let _: () = redis::cmd("SET")
            .arg(key)
            .arg(item)
            .query_async(&mut conn)
            .await?;

        Ok(())
    }

    async fn retrieve_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;
        let data: Bytes = redis::cmd("GET").arg(key).query_async(&mut conn).await?;

        Ok(data)
    }
}

#[async_trait::async_trait]
impl crate::BatchingDistributedCache for RedisDistributedCache {
    async fn cache_batch<'a, I>(&self, items: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, Bytes)> + Send,
    {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;

        let kvps: Vec<(&'a str, Vec<u8>)> =
            items.into_iter().map(|(k, v)| (k, v.to_vec())).collect();

        let _: () = conn.mset(&kvps).await?;

        Ok(())
    }

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<(&'a str, Bytes)>>
    where
        I: IntoIterator<Item = &'a str> + Send,
    {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;

        let keys_vec: Vec<&'a str> = keys.into_iter().collect();

        let values: Vec<Option<Bytes>> = conn.mget(&keys_vec).await?;
        let pairs = keys_vec
            .into_iter()
            .zip(values.into_iter().flatten())
            .collect();

        Ok(pairs)
    }
}
