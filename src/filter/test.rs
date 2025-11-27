use chrono::{NaiveDate, NaiveDateTime};
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

async fn next_err<S>(stream: &mut S, err: Error)
where
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    assert_eq!(stream.next().await.unwrap().unwrap_err(), err);
}

async fn no_record<S>(stream: &mut S)
where
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    assert!(stream.next().await.is_none());
}

fn assert_time_on_only(rec: &Record, expected_on: NaiveDateTime) {
    let time_on = rec.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, expected_on);
    assert!(rec.get(":time_off").is_none());
}

fn assert_both_times(
    rec: &Record, expected_on: NaiveDateTime, expected_off: NaiveDateTime,
) {
    let time_on = rec.get(":time_on").unwrap().as_datetime().unwrap();
    assert_eq!(time_on, expected_on);
    let time_off = rec.get(":time_off").unwrap().as_datetime().unwrap();
    assert_eq!(time_off, expected_off);
}

fn duplicate_key_error(key: &str, record: Record) -> Error {
    Error::DuplicateKey {
        key: key.to_string(),
        record,
    }
}

fn parse_many_ignore<'a, F, S>(adif: &'a str, ignore_partial: bool, f: F) -> S
where
    F: FnOnce(TrickleStream<RecordStream<TagStream<&'a [u8]>>>) -> S,
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    let stream = RecordStream::new(adif.as_bytes(), ignore_partial);
    let stream = TrickleStream::new(stream);
    f(stream)
}

fn parse_many<'a, F, S>(adif: &'a str, f: F) -> S
where
    F: FnOnce(TrickleStream<RecordStream<TagStream<&'a [u8]>>>) -> S,
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    parse_many_ignore(adif, true, f)
}

async fn parse_one<'a, F, S>(adif: &'a str, f: F) -> Record
where
    F: FnOnce(TrickleStream<RecordStream<TagStream<&'a [u8]>>>) -> S,
    S: Stream<Item = Result<Record, Error>> + Unpin,
{
    let mut s = parse_many(adif, f);
    let rec = next(&mut s).await;
    no_record(&mut s).await;
    rec
}

async fn parse_norm_mode(adif: &str) -> Record {
    parse_one(adif, normalize_mode).await
}

async fn parse_norm_band(adif: &str) -> Record {
    parse_one(adif, normalize_band).await
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
async fn normalize_mode_duplicate_key() {
    let stream = RecordStream::new("<mode:3>FT8<eor>".as_bytes(), true);
    let stream = normalize_mode(stream);
    let mut stream = normalize_mode(stream);
    let err = stream.next().await.unwrap().unwrap_err();

    let mut expected_record = Record::new();
    expected_record.insert("mode", "FT8").unwrap();
    expected_record.insert(":mode", "FT8").unwrap();

    assert_eq!(
        err,
        Error::DuplicateKey {
            key: ":mode".to_string(),
            record: expected_record,
        }
    );
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
async fn normalize_band_duplicate_key() {
    let stream = RecordStream::new("<band:3>20m<eor>".as_bytes(), true);
    let stream = normalize_band(stream);
    let mut stream = normalize_band(stream);
    let err = stream.next().await.unwrap().unwrap_err();

    let mut expected_record = Record::new();
    expected_record.insert("band", "20m").unwrap();
    expected_record.insert(":band", "20M").unwrap();

    assert_eq!(
        err,
        Error::DuplicateKey {
            key: ":band".to_string(),
            record: expected_record,
        }
    );
}

#[tokio::test]
async fn normalize_times_duplicate_key() {
    let mut count = 0;
    let mut s = parse_many(
        "<qso_date:8>20240101<time_on:6>120000<eor>
         <qso_date:8>20240101<time_on:6>120000<qso_date_off:8>20240101<time_off:6>130000<eor>
         <qso_date:8:d>20231215<time_on:6:t>143000<eor>
         <qso_date:8>20231215<time_on:6>143000<eor>
         <qso_date:8>20231215<time_on:6>143000<time_off:6>153000<eor>
         <qso_date:8>20231215<time_on:6>233000<time_off:6>001500<eor>
         <qso_date:8>20231215<time_on:6>233000<qso_date_off:8>20231216<time_off:6>013000<eor>
         <qso_date:8>20231231<time_on:6>233000<qso_date_off:8>20240101<time_off:6>003000<eor>
         <qso_date:8>20231215<eor>",
        |s| {
            normalize_times(s.normalize(move |r| {
                count += 1;
                if count == 1 {
                    r.insert(":time_on", "")
                } else if count == 2 {
                    r.insert(":time_off", "")
                } else {
                    Ok(())
                }
            }))
        },
    );

    let mut expected_record = Record::new();
    expected_record.insert("qso_date", "20240101").unwrap();
    expected_record.insert("time_on", "120000").unwrap();
    expected_record.insert(":time_on", "").unwrap();
    next_err(&mut s, duplicate_key_error(":time_on", expected_record)).await;

    let mut expected_record = Record::new();
    expected_record.insert("qso_date", "20240101").unwrap();
    expected_record.insert("time_on", "120000").unwrap();
    expected_record.insert("qso_date_off", "20240101").unwrap();
    expected_record.insert("time_off", "130000").unwrap();
    expected_record.insert(":time_off", "").unwrap();
    expected_record
        .insert(":time_on", dt(2024, 1, 1, 12, 0, 0))
        .unwrap();
    next_err(&mut s, duplicate_key_error(":time_off", expected_record)).await;

    let rec = next(&mut s).await;
    assert_time_on_only(&rec, dt(2023, 12, 15, 14, 30, 0));

    let rec = next(&mut s).await;
    assert_time_on_only(&rec, dt(2023, 12, 15, 14, 30, 0));

    let rec = next(&mut s).await;
    assert_both_times(
        &rec,
        dt(2023, 12, 15, 14, 30, 0),
        dt(2023, 12, 15, 15, 30, 0),
    );

    let rec = next(&mut s).await;
    assert_both_times(
        &rec,
        dt(2023, 12, 15, 23, 30, 0),
        dt(2023, 12, 16, 0, 15, 0),
    );

    let rec = next(&mut s).await;
    assert_both_times(
        &rec,
        dt(2023, 12, 15, 23, 30, 0),
        dt(2023, 12, 16, 1, 30, 0),
    );

    let rec = next(&mut s).await;
    assert_both_times(
        &rec,
        dt(2023, 12, 31, 23, 30, 0),
        dt(2024, 1, 1, 0, 30, 0),
    );

    let rec = next(&mut s).await;
    assert_eq!(
        rec.get("qso_date").unwrap().as_date().unwrap(),
        NaiveDate::from_ymd_opt(2023, 12, 15).unwrap()
    );
    assert!(rec.get("time_on").is_none());
    assert!(rec.get("time_off").is_none());
    assert!(rec.get(":time_on").is_none());
    assert!(rec.get(":time_off").is_none());

    no_record(&mut s).await;
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
async fn trickle_exclude_callsigns() {
    let stream = RecordStream::new(
        "<call:4>W1AW<eor><call:5>AB9BH<eor><call:4>W6RQ<eor>".as_bytes(),
        true,
    );
    let trickled = TrickleStream::new(stream);
    let mut filtered = exclude_callsigns(trickled, &["W1AW", "W6RQ"]);

    let rec = next(&mut filtered).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
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
async fn normalize_error() {
    let mut s = parse_many_ignore(
        "<call:4>W1AW<eor><call:5>AB9BH<eor><bad",
        false,
        |s| {
            s.normalize(|r| {
                if r.get("call").unwrap().as_str() == "W1AW" {
                    Err(Error::Filter("invalid callsign".into()))
                } else {
                    Ok(())
                }
            })
        },
    );
    next_err(&mut s, Error::Filter("invalid callsign".into())).await;
    let rec = next(&mut s).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    next_err(&mut s, partial_data(1, 36, 35)).await;
    no_record(&mut s).await;
}

#[tokio::test]
async fn filter_error_passthrough() {
    let mut s = parse_many_ignore(
        "<call:4>W1AW<eor><call:5>AB9BH<eor><bad",
        false,
        |s| FilterExt::filter(s, |r| r.get("call").unwrap().as_str() != "W1AW"),
    );
    let rec = next(&mut s).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    next_err(&mut s, partial_data(1, 36, 35)).await;
    no_record(&mut s).await;
}

#[tokio::test]
async fn filter_end_of_stream() {
    let mut s =
        parse_many("<call:4>W1AW<eor>", |s| FilterExt::filter(s, |_| true));
    let rec = next(&mut s).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    no_record(&mut s).await;
}
