//! Criterion hot-path: parse -> normalise -> Order Book update, per message and per burst.
use barter_data::{
    books::OrderBook, exchange::binance::spot::l2::BinanceSpotOrderBookL2Update,
    subscription::book::OrderBookEvent,
};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rust_decimal_macros::dec;

fn parse_fixture() -> BinanceSpotOrderBookL2Update {
    serde_json::from_str(include_str!(
        "../tests/fixtures/binance_spot_depth_update.json"
    ))
    .unwrap()
}

fn hot_path(c: &mut Criterion) {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/binance_spot_depth_update.json"
    ))
    .unwrap();

    let mut group = c.benchmark_group("parse_normalise_book");
    group.warm_up_time(std::time::Duration::from_millis(300));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(20);

    group.bench_function("per_message", |b| {
        b.iter(|| {
            let update: BinanceSpotOrderBookL2Update =
                serde_json::from_str(black_box(&fixture)).unwrap();
            let mut book = OrderBook::new(
                0,
                None,
                Vec::<(rust_decimal::Decimal, rust_decimal::Decimal)>::new(),
                Vec::new(),
            );
            book.update(&OrderBookEvent::Update(OrderBook::new(
                update.last_update_id,
                None,
                update.bids,
                update.asks,
            )));
            black_box(book.sequence())
        });
    });

    for burst in [1usize, 64] {
        group.throughput(Throughput::Elements(burst as u64));
        group.bench_with_input(BenchmarkId::new("burst", burst), &burst, |b, &burst| {
            b.iter(|| {
                let mut book = OrderBook::new(
                    0,
                    None,
                    vec![(dec!(1209.67), dec!(1.0))],
                    vec![(dec!(1210.00), dec!(1.0))],
                );
                for _ in 0..burst {
                    let update = parse_fixture();
                    book.update(&OrderBookEvent::Update(OrderBook::new(
                        update.last_update_id,
                        None,
                        update.bids,
                        update.asks,
                    )));
                }
                black_box(book.sequence())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, hot_path);
criterion_main!(benches);
