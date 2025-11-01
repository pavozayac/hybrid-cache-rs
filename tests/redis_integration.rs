use bytes::Bytes;
use hybrid_cache_rs::{BatchingDistributedCache, DistributedCache};
use testcontainers::runners::AsyncRunner;

use std::time::Duration;

#[tokio::test]
async fn redis_cache_integration() -> anyhow::Result<()> {
    // Start a docker-backed Redis container using testcontainers
    let node = testcontainers_modules::redis::Redis::default()
        .start()
        .await?;

    // Obtain the mapped port and build a redis URL
    let host = node.get_host_port_ipv4(6379).await?;
    let redis_url = format!("redis://127.0.0.1:{}", host);

    // Create the cache implementation
    let cache = hybrid_cache_rs::redis_impl::RedisDistributedCache::new(&redis_url)?;

    // Small sleep to ensure redis is ready to accept connections (testcontainers wait-for isn't always immediate)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Single-item cache/retrieve
    let key = "test:key:1";
    let payload = b"hello-world";
    cache.cache_bytes(key, payload).await?;

    let got = cache.retrieve_bytes(key).await?;
    assert_eq!(&got[..], payload);

    // Batch operations
    let items = vec![("k1", Bytes::from("v1")), ("k2", Bytes::from("v2"))];
    cache.cache_batch(items.clone()).await?;

    let keys = vec!["k1", "k2", "missing"];
    let vals = cache.retrieve_batch(keys).await?;

    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0].as_ref().map(|b| &b[..]), Some(b"v1" as &[u8]));
    assert_eq!(vals[1].as_ref().map(|b| &b[..]), Some(b"v2" as &[u8]));
    assert!(vals[2].is_none());

    Ok(())
}
