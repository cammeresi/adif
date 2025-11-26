use std::io;

use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;

use super::*;

pub(crate) mod helpers;
use helpers::*;

#[test]
fn tag_as_field_returns_some_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(tag.as_field().is_some());
    assert_eq!(tag.as_field().unwrap().name(), "call");
}

#[test]
fn tag_as_field_returns_none_for_eoh() {
    let tag = Tag::Eoh;
    assert!(tag.as_field().is_none());
}

#[test]
fn tag_as_field_returns_none_for_eor() {
    let tag = Tag::Eor;
    assert!(tag.as_field().is_none());
}

#[test]
fn tag_is_eoh_true_for_eoh() {
    let tag = Tag::Eoh;
    assert!(tag.is_eoh());
}

#[test]
fn tag_is_eoh_false_for_eor() {
    let tag = Tag::Eor;
    assert!(!tag.is_eoh());
}

#[test]
fn tag_is_eoh_false_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(!tag.is_eoh());
}

#[test]
fn tag_is_eor_true_for_eor() {
    let tag = Tag::Eor;
    assert!(tag.is_eor());
}

#[test]
fn tag_is_eor_false_for_eoh() {
    let tag = Tag::Eoh;
    assert!(!tag.is_eor());
}

#[test]
fn tag_is_eor_false_for_field() {
    let field = Field::new("call", "W1AW");
    let tag = Tag::Field(field);
    assert!(!tag.is_eor());
}

#[test]
fn as_bool_unsupported_types() {
    let d = Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    assert!(d.as_bool().is_none());

    let n = Datum::Number(Decimal::from(123));
    assert!(n.as_bool().is_none());

    let t = Datum::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap());
    assert!(t.as_bool().is_none());

    let s = Datum::String("abc".to_string());
    assert!(s.as_bool().is_none());
}

#[test]
fn as_number_unsupported_types() {
    let b = Datum::Boolean(true);
    assert!(b.as_number().is_none());

    let d = Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    assert!(d.as_number().is_none());

    let t = Datum::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap());
    assert!(t.as_number().is_none());
}

#[test]
fn as_date_unsupported_types() {
    let b = Datum::Boolean(false);
    assert!(b.as_date().is_none());

    let n = Datum::Number(Decimal::from(123));
    assert!(n.as_date().is_none());

    let t = Datum::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap());
    assert!(t.as_date().is_none());
}

#[test]
fn as_time_unsupported_types() {
    let d = Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    assert!(d.as_time().is_none());

    let b = Datum::Boolean(true);
    assert!(b.as_time().is_none());

    let n = Datum::Number(Decimal::from(456));
    assert!(n.as_time().is_none());
}

#[test]
fn as_datetime_unsupported_types() {
    let t = Datum::Time(NaiveTime::from_hms_opt(14, 30, 0).unwrap());
    assert!(t.as_datetime().is_none());

    let d = Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    assert!(d.as_datetime().is_none());

    let b = Datum::Boolean(false);
    assert!(b.as_datetime().is_none());
}

#[test]
fn error_eq_io_errors() {
    let e1 = Error::Io(io::Error::new(io::ErrorKind::NotFound, "test"));
    let e2 = Error::Io(io::Error::new(io::ErrorKind::NotFound, "test"));
    let e3 = Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "test"));

    assert_eq!(e1, e2);
    assert_ne!(e1, e3);
    assert_ne!(e1, invalid_format("msg", 1, 1, 0));
}

#[test]
fn position_display() {
    let pos = Position {
        line: 5,
        column: 12,
        byte: 42,
    };
    assert_eq!(pos.to_string(), "line 5, column 12 (byte 42)");
}

#[test]
fn into_fields() {
    let mut record = Record::new();
    record.insert("call", "W1AW").unwrap();
    record.insert("freq", Decimal::from(14)).unwrap();

    let fields: Vec<_> = record.into_fields().collect();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].0, "call");
    assert_eq!(fields[0].1.as_str(), "W1AW");
    assert_eq!(fields[1].0, "freq");
    assert_eq!(fields[1].1.as_number().unwrap(), Decimal::from(14));
}

#[test]
fn to_cabrillo() {
    let s = Datum::String("test".to_string());
    assert_eq!(s.to_cabrillo(), "test");

    let b = Datum::Boolean(true);
    assert_eq!(b.to_cabrillo(), "Y");
    let b = Datum::Boolean(false);
    assert_eq!(b.to_cabrillo(), "N");

    let n = Datum::Number(Decimal::from(123));
    assert_eq!(n.to_cabrillo(), "123");

    let d = Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    assert_eq!(d.to_cabrillo(), "2024-01-15");

    let t = Datum::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap());
    assert_eq!(t.to_cabrillo(), "1234");

    let dt = Datum::DateTime(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap(),
    );
    assert_eq!(dt.to_cabrillo(), "2024-01-15 1234");
}
