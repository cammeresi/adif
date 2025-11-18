use super::*;
use crate::{Datum, Field, Record, RecordStream, Tag};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;

async fn encode_tag(tag: Tag, types: OutputTypes) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut sink = TagEncoder::with_types(types).tag_sink_with(&mut buf);
    sink.send(tag).await.unwrap();
    sink.close().await.unwrap();
    buf
}

async fn encode_field(
    value: Datum, always: &str, only_non_string: &str, never: &str,
) {
    let field = Field {
        name: "f".to_string(),
        value,
    };
    let out = encode_tag(Tag::Field(field.clone()), OutputTypes::Always).await;
    assert_eq!(out.as_slice(), always.as_bytes(), "failed on Always");
    let out =
        encode_tag(Tag::Field(field.clone()), OutputTypes::OnlyNonString).await;
    assert_eq!(
        out.as_slice(),
        only_non_string.as_bytes(),
        "failed on OnlyNonString"
    );
    let out = encode_tag(Tag::Field(field), OutputTypes::Never).await;
    assert_eq!(out.as_slice(), never.as_bytes(), "failed on Never");
}

async fn encode_record(record: Record, types: OutputTypes) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, types);
    sink.send(record).await.unwrap();
    sink.close().await.unwrap();
    buf
}

#[tokio::test]
async fn encode_eoh() {
    let output = encode_tag(Tag::Eoh, OutputTypes::Never).await;
    assert_eq!(output, b"<eoh>\n");
}

#[tokio::test]
async fn encode_eor() {
    let output = encode_tag(Tag::Eor, OutputTypes::Never).await;
    assert_eq!(output, b"<eor>\n");
}

#[tokio::test]
async fn encode_boolean() {
    encode_field(Datum::Boolean(true), "<f:1:b>Y", "<f:1:b>Y", "<f:1>Y").await;
    encode_field(Datum::Boolean(false), "<f:1:b>N", "<f:1:b>N", "<f:1>N").await;
}

#[tokio::test]
async fn encode_number() {
    encode_field(
        Datum::Number(Decimal::from_str("14.074").unwrap()),
        "<f:6:n>14.074",
        "<f:6:n>14.074",
        "<f:6>14.074",
    )
    .await;
}

#[tokio::test]
async fn encode_date() {
    encode_field(
        Datum::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
        "<f:8:d>20240115",
        "<f:8:d>20240115",
        "<f:8>20240115",
    )
    .await;
}

#[tokio::test]
async fn encode_time() {
    encode_field(
        Datum::Time(chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap()),
        "<f:6:t>143000",
        "<f:6:t>143000",
        "<f:6>143000",
    )
    .await;
}

#[tokio::test]
async fn encode_string() {
    encode_field("foo".into(), "<f:3:s>foo", "<f:3>foo", "<f:3>foo").await;
}

#[tokio::test]
async fn datetime_errors() {
    let field = Field {
        name: "timestamp".into(),
        value: Datum::DateTime(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(14, 30, 0)
                .unwrap(),
        ),
    };
    let mut buf = Vec::new();
    let mut sink = TagEncoder::new().tag_sink_with(&mut buf);
    let result = sink.send(Tag::Field(field)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn record_sink_basic() {
    let mut record = Record::new();
    record.insert("call".into(), "W1AW".into()).unwrap();
    record.insert("freq".into(), "14.074".into()).unwrap();

    let buf = encode_record(record, OutputTypes::Never).await;

    let output = String::from_utf8(buf).unwrap();
    assert!(output.contains("<call:4>W1AW"));
    assert!(output.contains("<freq:6>14.074"));
    assert!(output.contains("<eor>\n"));
}

#[tokio::test]
async fn record_sink_header() {
    let mut header = Record::new_header();
    header.insert("adifver".into(), "3.1.4".into()).unwrap();

    let buf = encode_record(header, OutputTypes::Never).await;

    let output = String::from_utf8(buf).unwrap();
    assert!(output.contains("<adifver:5>3.1.4"));
    assert!(output.contains("<eoh>\n"));
}

#[tokio::test]
async fn record_roundtrip() {
    let mut record = Record::new();
    record.insert("call".into(), "AB9BH".into()).unwrap();
    record
        .insert(
            "freq".into(),
            Datum::Number(Decimal::from_str("7.074").unwrap()),
        )
        .unwrap();

    let buf = encode_record(record.clone(), OutputTypes::OnlyNonString).await;

    let mut stream = RecordStream::new(&buf[..], true);
    let parsed = stream.next().await.unwrap().unwrap();

    assert_eq!(parsed.get("call").unwrap().as_str().unwrap(), "AB9BH");
    assert_eq!(
        parsed.get("freq").unwrap().as_number().unwrap(),
        Decimal::from_str("7.074").unwrap()
    );
    assert_eq!(parsed, record);
}
