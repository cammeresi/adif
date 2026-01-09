use chrono::{Days, NaiveDate, NaiveTime};
use difa::{Datum, Error, OutputTypes, Record, RecordSink, RecordStream};
use futures::{SinkExt, StreamExt};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn field_name_strategy() -> impl Strategy<Value = String> {
    prop::char::ranges(vec![('a'..='z'), ('A'..='Z')].into()).prop_flat_map(
        |first| {
            prop::collection::vec(
                prop::char::ranges(
                    vec![('a'..='z'), ('A'..='Z'), ('0'..='9'), ('_'..='_')]
                        .into(),
                ),
                0..9,
            )
            .prop_map(move |rest| {
                let mut s = String::with_capacity(1 + rest.len());
                s.push(first);
                s.extend(rest);
                s
            })
        },
    )
}

fn string_datum_strategy() -> impl Strategy<Value = Datum> {
    any::<String>().prop_map(Datum::String)
}

fn string_datum_strategy_for_whitespace() -> impl Strategy<Value = Datum> {
    prop::collection::vec(
        prop_oneof![
            prop::char::range(' ', ';'), // space through ;
            prop::char::range('=', '='), // =
            prop::char::range('?', '~'), // ? through ~
        ],
        0..20,
    )
    .prop_map(|v| Datum::String(v.into_iter().collect()))
}

fn boolean_datum_strategy() -> impl Strategy<Value = Datum> {
    any::<bool>().prop_map(Datum::Boolean)
}

fn number_datum_strategy() -> impl Strategy<Value = Datum> {
    any::<Decimal>().prop_map(Datum::Number)
}

fn date_datum_strategy() -> impl Strategy<Value = Datum> {
    let base = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
    let max = 200 * 365 + 50; // 200 years + 50 leap days

    (0u64..=max).prop_filter_map("valid date", move |days| {
        base.checked_add_days(Days::new(days)).map(Datum::Date)
    })
}

fn time_datum_strategy() -> impl Strategy<Value = Datum> {
    (0u32..24, 0u32..60, 0u32..60).prop_map(|(h, m, s)| {
        Datum::Time(NaiveTime::from_hms_opt(h, m, s).unwrap())
    })
}

fn datum_strategy(whitespace: bool) -> impl Strategy<Value = Datum> {
    if whitespace {
        prop_oneof![
            string_datum_strategy_for_whitespace(),
            boolean_datum_strategy(),
            number_datum_strategy(),
            date_datum_strategy(),
            time_datum_strategy(),
        ]
        .boxed()
    } else {
        prop_oneof![
            string_datum_strategy(),
            boolean_datum_strategy(),
            number_datum_strategy(),
            date_datum_strategy(),
            time_datum_strategy(),
        ]
        .boxed()
    }
}

fn record_strategy(whitespace: bool) -> impl Strategy<Value = Record> {
    prop::collection::hash_map(
        field_name_strategy(),
        datum_strategy(whitespace),
        0..=10,
    )
    .prop_map(|fields| {
        let mut record = Record::new();
        for (name, datum) in fields {
            let _ = record.insert(name, datum);
        }
        record
    })
}

fn header_strategy(whitespace: bool) -> impl Strategy<Value = Record> {
    prop::collection::hash_map(
        field_name_strategy(),
        datum_strategy(whitespace),
        0..=3,
    )
    .prop_map(|fields| {
        let mut record = Record::new_header();
        for (name, datum) in fields {
            let _ = record.insert(name, datum);
        }
        record
    })
}

fn output_types_strategy() -> impl Strategy<Value = OutputTypes> {
    prop_oneof![Just(OutputTypes::Always), Just(OutputTypes::OnlyNonString),]
}

fn whitespace_injection_strategy()
-> impl Strategy<Value = Vec<(usize, Vec<u8>)>> {
    prop::collection::vec(
        (
            0usize..=5,
            prop::collection::vec(
                prop::sample::select(vec![b' ', b'\n', b'\t']),
                1..=3,
            ),
        ),
        3,
    )
}

async fn write_records(
    header: &Record, records: &[Record], output_types: OutputTypes,
    whitespace_injections: Option<&[(usize, Vec<u8>)]>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, output_types);

    sink.send(header.clone()).await.unwrap();
    for record in records {
        sink.send(record.clone()).await.unwrap();
    }
    sink.close().await.unwrap();

    let Some(whitespace_injections) = whitespace_injections else {
        return buf;
    };

    let mut result = Vec::new();
    let mut injections = whitespace_injections.iter().cycle();
    let mut tags_to_skip = 0;

    for &byte in &buf {
        if byte == b'<' {
            if tags_to_skip == 0 {
                let (skip, ws) = injections.next().unwrap();
                result.extend_from_slice(ws);
                tags_to_skip = *skip;
            } else {
                tags_to_skip -= 1;
            }
        }
        result.push(byte);
    }
    result
}

async fn test_roundtrip(
    header: Record, records: Vec<Record>, output_types: OutputTypes,
) {
    let buf = write_records(&header, &records, output_types, None).await;

    let mut stream = RecordStream::new(&buf[..], true);
    let parsed_header = stream.next().await.unwrap().unwrap();
    assert!(parsed_header.is_header());
    assert_eq!(parsed_header, header);
    for record in records {
        let parsed = stream.next().await.unwrap().unwrap();
        assert!(!parsed.is_header());
        assert_eq!(parsed, record);
    }
    assert!(stream.next().await.is_none());
}

fn assert_field_equals_coerced(parsed: &Datum, original: &Datum) {
    match original {
        Datum::String(s) => {
            assert_eq!(parsed.as_str().as_ref(), s);
        }
        Datum::Boolean(b) => {
            assert_eq!(parsed.as_bool().unwrap(), *b);
        }
        Datum::Number(n) => {
            assert_eq!(parsed.as_number().unwrap(), *n);
        }
        Datum::Date(d) => {
            assert_eq!(parsed.as_date().unwrap(), *d);
        }
        Datum::Time(t) => {
            assert_eq!(parsed.as_time().unwrap(), *t);
        }
        Datum::DateTime(_) => {
            unreachable!("DateTime should not be in test data");
        }
    }
}

fn assert_records_equal_coerced(parsed: &Record, original: &Record) {
    assert_eq!(parsed.fields().count(), original.fields().count());
    for (name, original) in original.fields() {
        let parsed = parsed.get(name).unwrap();
        assert_field_equals_coerced(parsed, original);
    }
}

async fn test_roundtrip_never(header: Record, records: Vec<Record>) {
    let buf = write_records(&header, &records, OutputTypes::Never, None).await;

    let mut stream = RecordStream::new(&buf[..], true);
    let parsed_header = stream.next().await.unwrap().unwrap();
    assert!(parsed_header.is_header());
    assert_records_equal_coerced(&parsed_header, &header);
    for record in records {
        let parsed = stream.next().await.unwrap().unwrap();
        assert!(!parsed.is_header());
        assert_records_equal_coerced(&parsed, &record);
    }
    assert!(stream.next().await.is_none());
}

async fn test_whitespace(
    header: Record, records: Vec<Record>, output_types: OutputTypes,
    whitespace_injections: Vec<(usize, Vec<u8>)>,
) {
    let buf = write_records(
        &header,
        &records,
        output_types,
        Some(&whitespace_injections),
    )
    .await;

    let mut stream = RecordStream::new(&buf[..], true);
    let parsed_header = stream.next().await.unwrap().unwrap();
    assert!(parsed_header.is_header());
    assert_eq!(parsed_header, header);
    for record in records {
        let parsed = stream.next().await.unwrap().unwrap();
        assert!(!parsed.is_header());
        assert_eq!(parsed, record);
    }
    assert!(stream.next().await.is_none());
}

async fn test_truncation_error_position(records: Vec<Record>) {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, OutputTypes::Never);

    for record in &records {
        sink.send(record.clone()).await.unwrap();
    }
    sink.close().await.unwrap();

    let pos = buf.len() * 3 / 4;
    let truncated = &buf[..pos];
    let mut stream = RecordStream::new(truncated, false);

    loop {
        match stream.next().await {
            None => break,
            Some(Ok(_)) => continue,
            Some(Err(Error::InvalidFormat { position, .. })) => {
                assert_eq!(buf.get(position.byte), Some(&b'<'));
                break;
            }
            Some(Err(e)) => {
                panic!("unexpected error: {}", e);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn roundtrip_typed(
        header in header_strategy(false),
        records in prop::collection::vec(record_strategy(false), 0..=20),
        output_types in output_types_strategy()
    ) {
        tokio_test::block_on(test_roundtrip(header, records, output_types));
    }

    #[test]
    fn roundtrip_untyped(
        header in header_strategy(false),
        records in prop::collection::vec(record_strategy(false), 0..=20)
    ) {
        tokio_test::block_on(test_roundtrip_never(header, records));
    }

    #[test]
    fn roundtrip_with_whitespace(
        header in header_strategy(true),
        records in prop::collection::vec(record_strategy(true), 0..=20),
        output_types in output_types_strategy(),
        whitespace in whitespace_injection_strategy()
    ) {
        let test =
            test_whitespace(header, records, output_types, whitespace);
        tokio_test::block_on(test);
    }

    #[test]
    fn truncation_error_position(
        records in prop::collection::vec(record_strategy(false), 1..=2)
    ) {
        tokio_test::block_on(test_truncation_error_position(records));
    }
}
