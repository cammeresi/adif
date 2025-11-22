use adif::{Datum, OutputTypes, Record, RecordSink, RecordStream};
use chrono::{Days, NaiveDate, NaiveTime};
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

fn datum_strategy() -> impl Strategy<Value = Datum> {
    prop_oneof![
        string_datum_strategy(),
        boolean_datum_strategy(),
        number_datum_strategy(),
        date_datum_strategy(),
        time_datum_strategy(),
    ]
}

fn record_strategy() -> impl Strategy<Value = Record> {
    prop::collection::hash_map(field_name_strategy(), datum_strategy(), 0..=10)
        .prop_map(|fields| {
            let mut record = Record::new();
            for (name, datum) in fields {
                let _ = record.insert(name, datum);
            }
            record
        })
}

fn header_strategy() -> impl Strategy<Value = Record> {
    prop::collection::hash_map(field_name_strategy(), datum_strategy(), 0..=3)
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

async fn test_roundtrip(
    header: Record, records: Vec<Record>, output_types: OutputTypes,
) {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, output_types);

    sink.send(header.clone()).await.unwrap();
    for record in &records {
        sink.send(record.clone()).await.unwrap();
    }
    sink.close().await.unwrap();

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
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, OutputTypes::Never);

    sink.send(header.clone()).await.unwrap();
    for record in &records {
        sink.send(record.clone()).await.unwrap();
    }
    sink.close().await.unwrap();

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn roundtrip_typed(
        header in header_strategy(),
        records in prop::collection::vec(record_strategy(), 0..=20),
        output_types in output_types_strategy()
    ) {
        tokio_test::block_on(test_roundtrip(header, records, output_types));
    }

    #[test]
    fn roundtrip_untyped(
        header in header_strategy(),
        records in prop::collection::vec(record_strategy(), 0..=20)
    ) {
        tokio_test::block_on(test_roundtrip_never(header, records));
    }
}
