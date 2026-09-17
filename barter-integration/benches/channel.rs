use barter_integration::channel::{LatencySamples, OverflowPolicy, mpsc_bounded};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::time::Instant;

fn channel_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("bounded_channel");
    for burst in [1usize, 64, 4096] {
        group.bench_with_input(
            BenchmarkId::new("per_message", burst),
            &burst,
            |b, &burst| {
                b.iter(|| {
                    let (tx, mut rx) = mpsc_bounded(burst.max(1), OverflowPolicy::DropOldest);
                    for value in 0..burst {
                        black_box(tx.try_send(value).unwrap());
                    }
                    let mut consumed = 0;
                    while let Some(value) = rx.try_recv() {
                        consumed += black_box(value);
                    }
                    black_box(consumed)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("burst_latency_percentiles", burst),
            &burst,
            |b, &burst| {
                b.iter(|| {
                    let (tx, mut rx) = mpsc_bounded(burst.max(1), OverflowPolicy::DropOldest);
                    let mut samples = LatencySamples::default();
                    for value in 0..burst {
                        let start = Instant::now();
                        tx.try_send(value).unwrap();
                        let _ = rx.try_recv();
                        samples.record(start);
                    }
                    black_box((samples.percentile(0.50), samples.percentile(0.95)));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, channel_benchmarks);
criterion_main!(benches);
