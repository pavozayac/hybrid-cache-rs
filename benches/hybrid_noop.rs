use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hybrid_cache_rs::{CachedRepresentation, HybridCache, NoopDistributedCache};
use rand::Rng;
use rand::distr::Alphanumeric;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

fn bench_single(c: &mut Criterion) {
    let noop = NoopDistributedCache::new();
    let cache = HybridCache::builder()
        .distributed_cache(noop)
        .cached_representation(CachedRepresentation::Binary)
        .build();

    let mut group = c.benchmark_group("hybrid_noop_single");

    group.bench_function("cache_and_retrieve", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let key = random_string(16);
                let value = TestData {
                    value: random_string(64),
                };
                cache.set((key.clone(), value.clone())).await;
                let _res: TestData = cache.get(key).await.unwrap();
            });
    });

    group.finish();
}

fn bench_batch(c: &mut Criterion) {
    let noop = NoopDistributedCache::new();
    let cache = HybridCache::builder()
        .distributed_cache(noop)
        .cached_representation(CachedRepresentation::Binary)
        .build();

    let mut group = c.benchmark_group("hybrid_noop_batch");

    for &size in &[10usize, 100usize, 1000usize] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let cache = cache.clone();
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| {
                    let value = cache.clone();
                    async move {
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

                        value.set(kvps.clone()).await;

                        let keys: Vec<String> = kvps.into_iter().map(|kv| kv.0).collect();
                        let _: Vec<(String, TestData)> =
                            value.get_many(keys).await.into_iter().collect();
                    }
                });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_single, bench_batch);
criterion_main!(benches);
