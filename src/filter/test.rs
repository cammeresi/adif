use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;

use super::*;
use crate::parse::{RecordStream, TagStream};
use crate::test::helpers::*;

fn dt(
    year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32,
) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, min, sec)
        .unwrap()
}

async fn next<S>(stream: &mut S) -> Record
where
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    stream.next().await.unwrap().unwrap()
}

async fn parse_with<'a, F, S>(adif: &'a str, f: F) -> Record
where
    F: FnOnce(RecordStream<TagStream<&'a [u8]>>) -> S,
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    let stream = RecordStream::new(adif.as_bytes(), true);
    let mut normalized = f(stream);
    next(&mut normalized).await
}

async fn parse_norm_times(adif: &str) -> Record {
    parse_with(adif, normalize_times).await
}

async fn parse_norm_mode(adif: &str) -> Record {
    parse_with(adif, normalize_mode).await
}

async fn parse_norm_band(adif: &str) -> Record {
    parse_with(adif, normalize_band).await
}

#[tokio::test]
async fn normalize_mode_from_mode() {
    let record = parse_norm_mode("<mode:3>SSB<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "SSB");
}

#[tokio::test]
async fn normalize_mode_from_app_lotw_mode() {
    let record = parse_norm_mode("<app_lotw_mode:3>FT8<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "FT8");
}

#[tokio::test]
async fn normalize_mode_from_modegroup() {
    let record = parse_norm_mode("<app_lotw_modegroup:4>RTTY<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "RTTY");
}

#[tokio::test]
async fn normalize_mode_precedence() {
    let record = parse_norm_mode("<mode:3>SSB<app_lotw_mode:3>FT8<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "SSB");
}

#[tokio::test]
async fn normalize_mode_no_source() {
    let record = parse_norm_mode("<call:4>W1AW<eor>").await;
    assert!(record.get(":mode").is_none());
}

#[tokio::test]
async fn normalize_mode_mfsk_with_ft4() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>FT4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "FT4");
}

#[tokio::test]
async fn normalize_mode_mfsk_with_q65() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>Q65<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "Q65");
}

#[tokio::test]
async fn normalize_mode_mfsk_with_other_submode() {
    let record = parse_norm_mode("<mode:4>MFSK<submode:3>XXX<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "MFSK");
}

#[tokio::test]
async fn normalize_mode_mfsk_no_submode() {
    let record = parse_norm_mode("<mode:4>MFSK<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "MFSK");
}

#[tokio::test]
async fn normalize_mode_non_mfsk_with_submode() {
    let record = parse_norm_mode("<mode:3>SSB<submode:3>FT4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "SSB");
}

#[tokio::test]
async fn normalize_mode_case_insensitive() {
    let record = parse_norm_mode("<mode:4>mfsk<submode:3>ft4<eor>").await;
    assert_eq!(record.get(":mode").unwrap().as_str(), "ft4");
}

#[tokio::test]
async fn normalize_band_uppercase() {
    let record = parse_norm_band("<band:3>20m<eor>").await;
    assert_eq!(record.get(":band").unwrap().as_str(), "20M");
}

#[tokio::test]
async fn normalize_band_already_upper() {
    let record = parse_norm_band("<band:3>40M<eor>").await;
    assert_eq!(record.get(":band").unwrap().as_str(), "40M");
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

#[tokio::test]
async fn exclude_single_callsign() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_callsigns(stream, &["W1AW"]);
    let record = next(&mut filtered).await;
    assert_eq!(record.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_multiple_callsigns() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor><call:4>W6RQ<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_callsigns(stream, &["W1AW", "W6RQ"]);
    let record = next(&mut filtered).await;
    assert_eq!(record.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_callsigns_case_insensitive() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_callsigns(stream, &["w1aw"]);
    let record = next(&mut filtered).await;
    assert_eq!(record.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_callsigns_no_match() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_callsigns(stream, &["W6RQ"]);
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_callsigns_missing_call() {
    let stream = RecordStream::new(
        "<freq:6>14.070<eor><call:4>W1AW<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_callsigns(stream, &["W1AW"]);
    let rec = next(&mut filtered).await;
    assert!(rec.get("call").is_none());
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_header_removes_header() {
    let stream = RecordStream::new(
        "<adifver:5>3.1.4<eoh><call:4>W1AW<eor><call:5>AB9BH<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_header(stream);
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn exclude_header_no_header() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor>".as_bytes(),
        true,
    );
    let mut filtered = exclude_header(stream);
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    assert!(filtered.next().await.is_none());
}

#[tokio::test]
async fn normalize_error_passthrough() {
    let stream = RecordStream::new("<call:4>W1AW<eor><bad".as_bytes(), false);
    let mut normalized = stream.normalize(|_record| {});
    let rec = next(&mut normalized).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let err = normalized.next().await.unwrap().unwrap_err();
    assert_eq!(err, partial_data(1, 18, 17));
}

#[tokio::test]
async fn filter_error_passthrough() {
    let stream = RecordStream::new("<call:4>W1AW<eor><bad".as_bytes(), false);
    let mut filtered = FilterExt::filter(stream, |_record| true);
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let err = filtered.next().await.unwrap().unwrap_err();
    assert_eq!(err, partial_data(1, 18, 17));
}

#[tokio::test]
async fn normalize_end_of_stream() {
    let stream = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    let mut normalized = stream.normalize(|_record| {});
    let rec = next(&mut normalized).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    assert!(normalized.next().await.is_none());
}

#[tokio::test]
async fn filter_end_of_stream() {
    let stream = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    let mut filtered = FilterExt::filter(stream, |_record| true);
    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    assert!(filtered.next().await.is_none());
}
