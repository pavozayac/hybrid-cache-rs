use bytes::Bytes;
use hybrid_cache_rs::{BatchingDistributedCache, DistributedCache};
use rand::Rng;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

use std::time::Duration;

async fn redis_container() -> anyhow::Result<(ContainerAsync<Redis>, String)> {
    // Start a docker-backed Redis container using testcontainers
    let node = testcontainers_modules::redis::Redis::default()
        .start()
        .await?;

    // Obtain the mapped port and build a redis URL
    let host = node.get_host_port_ipv4(6379).await?;
    let redis_url = format!("redis://127.0.0.1:{host}");

    Ok((node, redis_url))
}

#[tokio::test]
async fn redis_cache_single_operations() -> anyhow::Result<()> {
    let (_container, redis_url) = redis_container().await?;
    // Create the cache implementation
    let cache = hybrid_cache_rs::redis_impl::RedisDistributedCache::new(&redis_url)?;

    // Small sleep to ensure redis is ready to accept connections (testcontainers wait-for isn't always immediate)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Single-item cache/retrieve
    let key = "test:key:1";
    let payload = b"hello-world";
    cache.cache_bytes(key, payload).await?;

    let got = cache.retrieve_bytes(key).await?;
    assert_eq!(&*got, payload);

    Ok(())
}

#[tokio::test]
async fn redis_cache_batch_operations() -> anyhow::Result<()> {
    let (_container, redis_url) = redis_container().await?;

    // Create the cache implementation
    let cache = hybrid_cache_rs::redis_impl::RedisDistributedCache::new(&redis_url)?;

    // Small sleep to ensure redis is ready to accept connections (testcontainers wait-for isn't always immediate)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Batch operations
    let items = vec![("k1", Bytes::from("v1")), ("k2", Bytes::from("v2"))];
    cache.cache_batch(items.clone()).await?;

    let keys = vec!["k1", "k2", "missing"];
    let vals = cache.retrieve_batch(keys).await?;

    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0].1, b"v1" as &[u8]);
    assert_eq!(vals[1].1, b"v2" as &[u8]);

    Ok(())
}

use rand::distr::Alphanumeric;

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct TestData {
    value: String,
}

fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[tokio::test]
async fn redis_cache_1000_random_hybrid() -> anyhow::Result<()> {
    let (_container, redis_url) = redis_container().await?;

    // Create the distributed cache implementation
    let distributed = hybrid_cache_rs::redis_impl::RedisDistributedCache::new(&redis_url)?;

    // Build a hybrid cache that uses Redis as the distributed layer
    let cache = hybrid_cache_rs::HybridCache::builder()
        .distributed_cache(distributed)
        .cached_representation(hybrid_cache_rs::CachedRepresentation::Binary)
        .build();

    // Small sleep to ensure redis is ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    let size = 1000usize;
    let kvps: Vec<(String, TestData)> = (0..size)
        .map(|_| {
            (
                random_string(16),
                TestData {
                    value: random_string(64),
                },
            )
        })
        .collect();

    // Cache them using the hybrid cache (writes to in-memory and distributed)
    cache.set_many(kvps.clone()).await;

    // Retrieve by keys
    let keys: Vec<String> = kvps.iter().map(|kv| kv.0.clone()).collect();
    let retrieved: Vec<(String, TestData)> = cache.get_many(keys).await.into_iter().collect();

    // Expect all items to be retrieved from in-memory cache
    assert_eq!(retrieved.len(), size);

    // spot-check some entries
    for i in 0..size {
        assert_eq!(retrieved[i].1, kvps[i].1);
    }

    Ok(())
}
