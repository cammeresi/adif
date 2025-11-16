use super::*;
use crate::parse::RecordStream;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;

fn dt(
    year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32,
) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, min, sec)
        .unwrap()
}

async fn parse_norm_times(adif_data: &str) -> Record {
    let stream = RecordStream::new(adif_data.as_bytes(), true);
    let mut normalized = normalize_times(stream);
    normalized.next().await.unwrap().unwrap()
}

async fn parse_norm_mode(adif_data: &str) -> Record {
    let stream = RecordStream::new(adif_data.as_bytes(), true);
    let mut normalized = normalize_mode(stream);
    normalized.next().await.unwrap().unwrap()
}

async fn parse_norm_band(adif_data: &str) -> Record {
    let stream = RecordStream::new(adif_data.as_bytes(), true);
    let mut normalized = normalize_band(stream);
    normalized.next().await.unwrap().unwrap()
}

#[tokio::test]
async fn duplicate_key_error() {
    let stream = RecordStream::new(
        "<qso_date:8>20231215<time_on:6>143000<eor>".as_bytes(),
        true,
    );
    let mut normalized = normalize_times(stream);
    let mut record = normalized.next().await.unwrap().unwrap();

    let err = record
        .insert(":time_on".to_string(), Datum::String("test".to_string()))
        .unwrap_err();
    match err {
        Error::InvalidFormat(s) => assert_eq!(s, "duplicate key: :time_on"),
        _ => panic!("expected InvalidFormat error"),
    }
}

#[tokio::test]
async fn normalize_mode_from_mode() {
    let record = parse_norm_mode("<mode:3>SSB<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "SSB");
}

#[tokio::test]
async fn normalize_mode_from_app_lotw_mode() {
    let record = parse_norm_mode("<app_lotw_mode:3>FT8<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "FT8");
}

#[tokio::test]
async fn normalize_mode_from_modegroup() {
    let record = parse_norm_mode("<app_lotw_modegroup:4>RTTY<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "RTTY");
}

#[tokio::test]
async fn normalize_mode_precedence() {
    let record = parse_norm_mode("<mode:3>SSB<app_lotw_mode:3>FT8<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "SSB");
}

#[tokio::test]
async fn normalize_mode_no_source() {
    let record = parse_norm_mode("<call:4>W1AW<eor>").await;
    assert!(record.get(":mode").is_none());
}

#[tokio::test]
async fn normalize_mode_mfsk_with_ft4() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>FT4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "FT4");
}

#[tokio::test]
async fn normalize_mode_mfsk_with_q65() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>Q65<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "Q65");
}

#[tokio::test]
async fn normalize_mode_mfsk_with_other_submode() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>XXX<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "MFSK");
}

#[tokio::test]
async fn normalize_mode_mfsk_no_submode() {
    let record = parse_norm_mode("<mode:4>MFSK<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "MFSK");
}

#[tokio::test]
async fn normalize_mode_non_mfsk_with_submode() {
    let record = parse_norm_mode("<mode:3>SSB<submode:3>FT4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "SSB");
}

#[tokio::test]
async fn normalize_mode_case_insensitive() {
    let record = parse_norm_mode("<mode:4>mfsk<submode:3>ft4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str().unwrap(), "ft4");
}

#[tokio::test]
async fn normalize_band_uppercase() {
    let record = parse_norm_band("<band:3>20m<eor>").await;
    assert_eq!(record.get(":band").unwrap().as_str().unwrap(), "20M");
}

#[tokio::test]
async fn normalize_band_already_upper() {
    let record = parse_norm_band("<band:3>40M<eor>").await;
    assert_eq!(record.get(":band").unwrap().as_str().unwrap(), "40M");
}

#[tokio::test]
async fn normalize_band_no_band() {
    let record = parse_norm_band("<call:4>W1AW<eor>").await;
    assert!(record.get(":band").is_none());
}

#[tokio::test]
async fn normalize_times_typed() {
    let record =
        parse_norm_times("<qso_date:8:d>20231215<time_on:6:t>143000<eor>")
            .await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 15, 14, 30, 0));
    assert!(record.get(":time_off").is_none());
}

#[tokio::test]
async fn normalize_times_basic() {
    let record =
        parse_norm_times("<qso_date:8>20231215<time_on:6>143000<eor>").await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 15, 14, 30, 0));
    assert!(record.get(":time_off").is_none());
}

#[tokio::test]
async fn normalize_times_with_time_off_same_day() {
    let record = parse_norm_times(
        "<qso_date:8>20231215<time_on:6>143000<time_off:6>153000<eor>",
    )
    .await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 15, 14, 30, 0));
    let time_off = record.get(":time_off").unwrap().as_datetime().unwrap();
    assert_eq!(time_off, dt(2023, 12, 15, 15, 30, 0));
}

#[tokio::test]
async fn normalize_times_with_time_off_next_day() {
    let record = parse_norm_times(
        "<qso_date:8>20231215<time_on:6>233000<time_off:6>001500<eor>",
    )
    .await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 15, 23, 30, 0));
    let time_off = record.get(":time_off").unwrap().as_datetime().unwrap();
    assert_eq!(time_off, dt(2023, 12, 16, 0, 15, 0));
}

#[tokio::test]
async fn normalize_times_with_qso_date_off() {
    let record = parse_norm_times(
        "<qso_date:8>20231215<time_on:6>233000<qso_date_off:8>20231216<time_off:6>013000<eor>",
    )
    .await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 15, 23, 30, 0));
    let time_off = record.get(":time_off").unwrap().as_datetime().unwrap();
    assert_eq!(time_off, dt(2023, 12, 16, 1, 30, 0));
}

#[tokio::test]
async fn normalize_times_with_qso_date_off_midnight_cross() {
    let record = parse_norm_times(
        "<qso_date:8>20231231<time_on:6>233000<qso_date_off:8>20240101<time_off:6>003000<eor>",
    )
    .await;

    let time_on = record.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, dt(2023, 12, 31, 23, 30, 0));
    let time_off = record.get(":time_off").unwrap().as_datetime().unwrap();
    assert_eq!(time_off, dt(2024, 1, 1, 0, 30, 0));
}

#[tokio::test]
async fn normalize_times_missing_date() {
    let record = parse_norm_times("<time_on:6>143000<eor>").await;

    assert_eq!(
        record.get("time_on").unwrap().as_time().unwrap(),
        NaiveTime::from_hms_opt(14, 30, 0).unwrap()
    );
}

#[tokio::test]
async fn normalize_times_missing_time_on() {
    let record = parse_norm_times("<qso_date:8>20231215<eor>").await;

    assert_eq!(
        record.get("qso_date").unwrap().as_date().unwrap(),
        NaiveDate::from_ymd_opt(2023, 12, 15).unwrap()
    );
    assert!(record.get("time_on").is_none());
    assert!(record.get("time_off").is_none());
}
