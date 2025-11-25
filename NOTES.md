### Coverage anomalies

Analysis with the `#[coverage(off)]` attribute in Rust nightly has
revealed that the coverage anomalies are:

- `<Normalize<S, F> as Stream>::poll_next (two regions)`
- `<Filter<S, F> as Stream>::poll_next (two regions)`
- `<RecordStream<S> as Stream>::poll_next (one region)`

