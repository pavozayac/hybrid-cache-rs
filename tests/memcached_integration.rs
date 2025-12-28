use bytes::Bytes;
use hybrid_cache_rs::{BatchingDistributedCache, DistributedCache};
use rand::Rng;
use testcontainers::{ContainerAsync, GenericImage, runners::AsyncRunner};

use std::time::Duration;

async fn memcached_container() -> anyhow::Result<(ContainerAsync<GenericImage>, String)> {
    let node = testcontainers::GenericImage::new("memcached", "1.6-alpine")
        .start()
        .await?;

    let host = node.get_host_port_ipv4(11211).await?;
    let memcached_url = format!("memcache://127.0.0.1:{host}");

    Ok((node, memcached_url))
}

#[tokio::test]
async fn memcached_impl_single_operations() -> anyhow::Result<()> {
    let (_container, memcached_url) = memcached_container().await?;

    let cache = hybrid_cache_rs::MemcachedDistributedCache::new(&memcached_url)?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let key = "test:key:1";
    let payload = b"hello-world";

    cache.cache_bytes(key, payload).await?;

    let got = cache.retrieve_bytes(key).await?;
    assert_eq!(&*got, payload);

    Ok(())
}

#[tokio::test]
async fn memcached_impl_batch_operations() -> anyhow::Result<()> {
    let (_container, memcached_url) = memcached_container().await?;

    let cache = hybrid_cache_rs::MemcachedDistributedCache::new(&memcached_url)?;

    tokio::time::sleep(Duration::from_millis(200)).await;

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
async fn memcached_impl_1000_random_hybrid() -> anyhow::Result<()> {
    let (_container, memcached_url) = memcached_container().await?;

    let distributed = hybrid_cache_rs::MemcachedDistributedCache::new(&memcached_url)?;

    let cache = hybrid_cache_rs::HybridCache::builder()
        .distributed_cache(distributed)
        .cached_representation(hybrid_cache_rs::CachedRepresentation::Binary)
        .build();

    // Small sleep to ensure memcached is ready
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

    cache.set_many(kvps.clone()).await;

    let keys: Vec<String> = kvps.iter().map(|kv| kv.0.clone()).collect();
    let retrieved: Vec<(String, TestData)> = cache.get_many(keys).await.into_iter().collect();

    assert_eq!(retrieved.len(), size);

    for i in 0..size {
        assert_eq!(retrieved[i].1, kvps[i].1);
    }

    Ok(())
}
