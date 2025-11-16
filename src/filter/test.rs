use super::*;
use crate::parse::RecordStream;
use futures::StreamExt;

#[tokio::test]
async fn duplicate_key_error() {
    let stream = RecordStream::new(
        "<qso_date:8>20231215<time_on:6>143000<eor>".as_bytes(),
        true,
    );
    let mut normalized = normalize_times(stream);
    let mut record = normalized.next().await.unwrap().unwrap();

    let err = record
        .insert(":time_on".to_string(), Data::String("test".to_string()))
        .unwrap_err();
    match err {
        Error::InvalidFormat(s) => assert_eq!(s, "duplicate key: :time_on"),
        _ => panic!("expected InvalidFormat error"),
    }
}

#[tokio::test]
async fn normalize_mode_from_mode() {
    let stream = RecordStream::new("<mode:3>SSB<eor>".as_bytes(), true);
    let mut normalized = normalize_mode(stream);
    let record = normalized.next().await.unwrap().unwrap();

    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "SSB");
}

#[tokio::test]
async fn normalize_mode_from_app_lotw_mode() {
    let stream =
        RecordStream::new("<app_lotw_mode:3>FT8<eor>".as_bytes(), true);
    let mut normalized = normalize_mode(stream);
    let record = normalized.next().await.unwrap().unwrap();

    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "FT8");
}

#[tokio::test]
async fn normalize_mode_from_modegroup() {
    let stream =
        RecordStream::new("<app_lotw_modegroup:4>RTTY<eor>".as_bytes(), true);
    let mut normalized = normalize_mode(stream);
    let record = normalized.next().await.unwrap().unwrap();

    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "RTTY");
}

#[tokio::test]
async fn normalize_mode_precedence() {
    let stream = RecordStream::new(
        "<mode:3>SSB<app_lotw_mode:3>FT8<eor>".as_bytes(),
        true,
    );
    let mut normalized = normalize_mode(stream);
    let record = normalized.next().await.unwrap().unwrap();

    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "SSB");
}

#[tokio::test]
async fn normalize_mode_no_source() {
    let stream = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    let mut normalized = normalize_mode(stream);
    let record = normalized.next().await.unwrap().unwrap();

    assert!(record.get(":mode").is_none());
}
