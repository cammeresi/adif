use std::str::FromStr;

use chrono::{NaiveDate, NaiveTime};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;

use super::{RecordSink, TagEncoder, TagSinkExt};
use crate::test::helpers::*;
use crate::{Datum, Field, OutputTypes, Record, RecordStream, Tag};

#[tokio::test]
async fn tag_sink() {
    let mut buf = Vec::new();
    let mut sink = (&mut buf).tag_sink();
    let field = Field::new("call", "W1AW");
    sink.send(Tag::Field(field)).await.unwrap();
    sink.close().await.unwrap();
    assert_eq!(buf, b"<call:4>W1AW");
}

#[tokio::test]
async fn tag_sink_with_types() {
    let mut buf = Vec::new();
    let mut sink = (&mut buf).tag_sink_with_types(OutputTypes::Always);
    let field = Field::new("call", "W1AW");
    sink.send(Tag::Field(field)).await.unwrap();
    sink.close().await.unwrap();
    assert_eq!(buf, b"<call:4:s>W1AW");
}

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
    let field = Field::new("f", value);
    let out = encode_tag(Tag::Field(field.clone()), OutputTypes::Always).await;
    assert_eq!(out.as_slice(), always.as_bytes());
    let out =
        encode_tag(Tag::Field(field.clone()), OutputTypes::OnlyNonString).await;
    assert_eq!(out.as_slice(), only_non_string.as_bytes());
    let out = encode_tag(Tag::Field(field), OutputTypes::Never).await;
    assert_eq!(out.as_slice(), never.as_bytes());
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
    encode_field(true.into(), "<f:1:b>Y", "<f:1:b>Y", "<f:1>Y").await;
    encode_field(false.into(), "<f:1:b>N", "<f:1:b>N", "<f:1>N").await;
}

#[tokio::test]
async fn encode_number() {
    encode_field(
        Decimal::from_str("14.074").unwrap().into(),
        "<f:6:n>14.074",
        "<f:6:n>14.074",
        "<f:6>14.074",
    )
    .await;
}

#[tokio::test]
async fn encode_date() {
    encode_field(
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().into(),
        "<f:8:d>20240115",
        "<f:8:d>20240115",
        "<f:8>20240115",
    )
    .await;
}

#[tokio::test]
async fn encode_time() {
    encode_field(
        NaiveTime::from_hms_opt(14, 30, 0).unwrap().into(),
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
    let field = Field::new(
        "timestamp",
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap(),
    );
    let mut buf = Vec::new();
    let mut sink = TagEncoder::new().tag_sink_with(&mut buf);
    let err = sink.send(Tag::Field(field)).await.unwrap_err();
    assert_eq!(
        err,
        cannot_output("DateTime", "split into date and time fields")
    );
}

#[tokio::test]
async fn encode_datetime_field_fails() {
    let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(14, 30, 45)
        .unwrap();
    let field = Field::new("qso_datetime", dt);

    let mut buf = Vec::new();
    let mut sink =
        TagEncoder::with_types(OutputTypes::Always).tag_sink_with(&mut buf);
    let err = sink.send(Tag::Field(field)).await.unwrap_err();
    assert_eq!(
        err,
        cannot_output("DateTime", "split into date and time fields")
    );
}

#[tokio::test]
async fn record_sink_basic() {
    let mut record = Record::new();
    record.insert("call", "W1AW").unwrap();
    record.insert("freq", "14.074").unwrap();

    let buf = encode_record(record, OutputTypes::Never).await;

    let output = String::from_utf8(buf).unwrap();
    assert!(output.contains("<call:4>W1AW"));
    assert!(output.contains("<freq:6>14.074"));
    assert!(output.contains("<eor>\n"));
}

#[tokio::test]
async fn record_sink_header() {
    let mut header = Record::new_header();
    header.insert("adifver", "3.1.4").unwrap();

    let buf = encode_record(header, OutputTypes::Never).await;

    let output = String::from_utf8(buf).unwrap();
    assert!(output.contains("<adifver:5>3.1.4"));
    assert!(output.contains("<eoh>\n"));
}

fn create_test_record(
    call: &str, date: &str, time: &str, freq: &str, mode: &str,
) -> Record {
    let mut record = Record::new();
    record.insert("call", call).unwrap();
    record.insert("qso_date", date).unwrap();
    record.insert("time_on", time).unwrap();
    record
        .insert("freq", Decimal::from_str(freq).unwrap())
        .unwrap();
    record.insert("mode", mode).unwrap();
    record
}

#[tokio::test]
async fn record_field_order() {
    let record1 =
        create_test_record("W1AW", "20240115", "143000", "14.074", "FT8");
    let record2 =
        create_test_record("AB9BH", "20240115", "150000", "7.074", "FT8");

    let buf1 = encode_record(record1, OutputTypes::Never).await;
    let buf2 = encode_record(record2, OutputTypes::Never).await;

    let output1 = String::from_utf8(buf1).unwrap();
    let output2 = String::from_utf8(buf2).unwrap();

    let fields1: Vec<&str> = output1.split('<').skip(1).collect();
    let fields2: Vec<&str> = output2.split('<').skip(1).collect();

    assert_eq!(fields1.len(), fields2.len());
    for (f1, f2) in fields1.iter().zip(fields2.iter()) {
        let name1 = f1.split(':').next().unwrap();
        let name2 = f2.split(':').next().unwrap();
        assert_eq!(name1, name2);
    }
}

#[tokio::test]
async fn record_roundtrip() {
    let mut record = Record::new();
    record.insert("call", "AB9BH").unwrap();
    record
        .insert("freq", Decimal::from_str("7.074").unwrap())
        .unwrap();

    let buf = encode_record(record.clone(), OutputTypes::OnlyNonString).await;

    let mut stream = RecordStream::new(&buf[..], true);
    let parsed = stream.next().await.unwrap().unwrap();

    assert_eq!(parsed.get("call").unwrap().as_str(), "AB9BH");
    assert_eq!(
        parsed.get("freq").unwrap().as_number().unwrap(),
        Decimal::from_str("7.074").unwrap()
    );
    assert_eq!(parsed, record);
}

#[tokio::test]
async fn encode_record_with_all_types() {
    let mut record = Record::new();
    record.insert("call", "W1AW").unwrap();
    record.insert("qsl", true).unwrap();
    record
        .insert("freq", Decimal::from_str("14.074").unwrap())
        .unwrap();
    record
        .insert("qso_date", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        .unwrap();
    record
        .insert("time_on", NaiveTime::from_hms_opt(14, 30, 0).unwrap())
        .unwrap();

    let mut buf = Vec::new();
    let mut sink = RecordSink::new(&mut buf);
    sink.send(record).await.unwrap();
    sink.close().await.unwrap();

    let output = String::from_utf8(buf).unwrap();
    assert_eq!(
        output,
        "<call:4>W1AW<qsl:1>Y<freq:6>14.074<qso_date:8>20240115<time_on:6>143000<eor>\n"
    );
}

#[tokio::test]
async fn encode_datetime_record_fails() {
    let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(14, 30, 45)
        .unwrap();
    let mut record = Record::new();
    record.insert("call", "W1AW").unwrap();
    record.insert("qso_datetime", dt).unwrap();

    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, OutputTypes::Always);
    let err = sink.send(record).await.unwrap_err();
    assert_eq!(
        err,
        cannot_output("DateTime", "split into date and time fields")
    );
}
