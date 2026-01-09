use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use difa::RecordStream;
use futures::StreamExt;
use std::hint::black_box;

mod common;

fn parse(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sizes = [10, 100, 1000, 10000, 100000];
    let datasets: Vec<_> = sizes
        .iter()
        .map(|&size| (size, rt.block_on(common::generate(size))))
        .collect();

    let mut group = c.benchmark_group("parse");

    for (size, data) in &datasets {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_function(format!("{}", size), |b| {
            b.to_async(&rt).iter(|| async {
                let mut stream = RecordStream::new(&data[..], true);
                let mut count = 0;
                while let Some(result) = stream.next().await {
                    let _ = result.unwrap();
                    count += 1;
                }
                black_box(count)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, parse);
criterion_main!(benches);
