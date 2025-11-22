use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

use super::*;
use crate::{Datum, Error, Field, Tag};

fn tags(s: &str) -> TagStream<&[u8]> {
    TagDecoder::new_stream(s.as_bytes(), true)
}

async fn next_field<R>(f: &mut TagStream<R>) -> Field
where
    R: AsyncRead + Unpin,
{
    let Tag::Field(field) = f.next().await.unwrap().unwrap() else {
        panic!("expected field");
    };
    field
}

async fn next_eoh<R>(f: &mut TagStream<R>)
where
    R: AsyncRead + Unpin,
{
    let Tag::Eoh = f.next().await.unwrap().unwrap() else {
        panic!("expected eoh");
    };
}

async fn no_tags<R>(f: &mut TagStream<R>)
where
    R: AsyncRead + Unpin,
{
    assert_eq!(f.next().await, None);
}

async fn next_record<S>(f: &mut RecordStream<S>, header: bool) -> Record
where
    S: Stream<Item = Result<Tag, Error>> + Unpin,
{
    let record = f.next().await.unwrap().unwrap();
    assert_eq!(record.is_header(), header);
    record
}

async fn no_records<S>(f: &mut RecordStream<S>)
where
    S: Stream<Item = Result<Tag, Error>> + Unpin,
{
    assert_eq!(f.next().await, None);
}

#[tokio::test]
#[should_panic(expected = "expected field")]
async fn next_field_panics_on_eoh() {
    let mut f = tags("<eoh>");
    next_field(&mut f).await;
}

#[tokio::test]
#[should_panic(expected = "expected field")]
async fn next_field_panics_on_eor() {
    let mut f = tags("<eor>");
    next_field(&mut f).await;
}

#[tokio::test]
#[should_panic(expected = "expected eoh")]
async fn next_eoh_panics_on_field() {
    let mut f = tags("<call:4>W1AW");
    next_eoh(&mut f).await;
}

#[tokio::test]
#[should_panic(expected = "expected eoh")]
async fn next_eoh_panics_on_eor() {
    let mut f = tags("<eor>");
    next_eoh(&mut f).await;
}

#[tokio::test]
async fn header() {
    let mut f = tags("Foo Bar Baz <adifver:5>3.1.1 <eoh>");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "adifver");
    assert_eq!(field.value().as_str(), "3.1.1");
    next_eoh(&mut f).await;
    no_tags(&mut f).await;
}

#[tokio::test]
async fn typed() {
    let mut f = tags("<foo:3:n>123");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(
        field.value(),
        &Datum::Number(Decimal::from_str("123").unwrap())
    );
    no_tags(&mut f).await;

    let mut f = tags("<foo:3:N>123");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(
        field.value(),
        &Datum::Number(Decimal::from_str("123").unwrap())
    );
    no_tags(&mut f).await;

    let mut f = tags("<bar:1:b>Y");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "bar");
    assert_eq!(field.value(), &Datum::Boolean(true));
    no_tags(&mut f).await;

    let mut f = tags("<bar:1:B>Y");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "bar");
    assert_eq!(field.value(), &Datum::Boolean(true));
    no_tags(&mut f).await;

    let mut f = tags("<qso_date:8:d>20240101");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "qso_date");
    assert_eq!(
        field.value(),
        &Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
    );
    no_tags(&mut f).await;

    let mut f = tags("<qso_date:8:D>20240101");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "qso_date");
    assert_eq!(
        field.value(),
        &Datum::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
    );
    no_tags(&mut f).await;

    let mut f = tags("<time_on:6:t>230000");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "time_on");
    assert_eq!(
        field.value(),
        &Datum::Time(NaiveTime::from_hms_opt(23, 0, 0).unwrap())
    );
    no_tags(&mut f).await;

    let mut f = tags("<time_on:6:T>230000");
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "time_on");
    assert_eq!(
        field.value(),
        &Datum::Time(NaiveTime::from_hms_opt(23, 0, 0).unwrap())
    );
    no_tags(&mut f).await;
}

#[tokio::test]
async fn fraction() {
    let mut f = tags("<freq:7:n>14.0705");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "freq");
    let freq = field.value().as_number().unwrap();
    assert_eq!(freq, Decimal::from_str("14.0705").unwrap());
    no_tags(&mut f).await;
}

#[tokio::test]
async fn uppercase() {
    let mut f = tags("<FOO:3>Bar");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "FOO");
    assert_eq!(field.value(), &"Bar".into());
    no_tags(&mut f).await;
}

#[tokio::test]
async fn underscore() {
    let mut f = tags("<my_tag:3>xyz");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "my_tag");
    assert_eq!(field.value(), &"xyz".into());
    no_tags(&mut f).await;
}

#[tokio::test]
async fn case_insensitive_lookup() {
    let mut s = RecordStream::new("<FOO:3>Bar<eor>".as_bytes(), true);
    let record = s.next().await.unwrap().unwrap();
    assert_eq!(record.get("FOO").unwrap().as_str(), "Bar");
    assert_eq!(record.get("foo").unwrap().as_str(), "Bar");
    assert_eq!(record.get("FoO").unwrap().as_str(), "Bar");
}

#[tokio::test]
async fn lotw_eof() {
    let mut f = tags("<foo:3>bar<app_lotw_eof><baz:3>qux");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(field.value(), &"bar".into());
    no_tags(&mut f).await;
}

#[tokio::test]
async fn partial_tag_ignore() {
    let s = "<foo:3>ba";
    for i in 0..s.len() {
        let mut f = tags(&s[..=i]);
        no_tags(&mut f).await;
    }
}

#[tokio::test]
async fn partial_tag_error() {
    let s = "<foo:3>ba";
    for i in 0..s.len() {
        let mut f = TagDecoder::new_stream(&s.as_bytes()[..=i], false);
        let err = f.next().await.unwrap().unwrap_err();
        assert_eq!(
            err,
            Error::InvalidFormat(Cow::Borrowed(
                "partial data at end of stream"
            ))
        );
    }
}

#[tokio::test]
async fn complete_tag_no_error() {
    let mut f = TagDecoder::new_stream("<foo:3>bar".as_bytes(), false);
    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(field.value().as_str(), "bar");
    no_tags(&mut f).await;
}

struct TrickleReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
}

impl TrickleReader {
    fn new(data: &str, chunk: usize) -> Self {
        Self {
            data: data.as_bytes().to_vec(),
            pos: 0,
            chunk,
        }
    }
}

impl AsyncRead for TrickleReader {
    fn poll_read(
        mut self: Pin<&mut Self>, _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let to_read = remaining.min(self.chunk).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        Poll::Ready(Ok(()))
    }
}

async fn try_trickle(chunk: usize) {
    let reader =
        TrickleReader::new("Foo <bar:3>baz <qux:5:n>12345 <eoh>", chunk);
    let mut f = TagDecoder::new_stream(reader, true);

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "bar");
    assert_eq!(field.value().as_str(), "baz");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "qux");
    assert_eq!(
        field.value().as_number().unwrap(),
        Decimal::from_str("12345").unwrap()
    );

    next_eoh(&mut f).await;
    no_tags(&mut f).await;
}

#[tokio::test]
async fn trickle() {
    try_trickle(1).await;
    try_trickle(2).await;
    try_trickle(3).await;
}

#[tokio::test]
async fn records() {
    let mut f =
        tags("<adifver:5>3.1.4 <eoh><call:4>W1AW<eor><call:5>AB9BH<eor>")
            .records();
    let rec = next_record(&mut f, true).await;
    assert_eq!(rec.get("adifver").unwrap().as_str(), "3.1.4");

    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    no_records(&mut f).await;
}

#[tokio::test]
async fn partial_record_ignore() {
    let mut f = tags("<call:4>W1AW<eor><call:5>AB9BH").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1AW<eor><call:5>AB9B").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1AW<eor>\n   ").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1A").records();
    no_records(&mut f).await;
}

#[tokio::test]
async fn partial_record_error() {
    let mut f = RecordStream::new("<call:4>W1A".as_bytes(), false);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Borrowed("partial data at end of stream"))
    );
}

#[tokio::test]
async fn record_stream() {
    let mut f = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    no_records(&mut f).await;
}

#[tokio::test]
async fn no_length() {
    let mut f = RecordStream::new("<call>W1AW<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Borrowed("call")));
}

#[tokio::test]
async fn too_many() {
    let mut f = RecordStream::new("<call:4:s:xxx>W1AW<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Borrowed("call:4:s:xxx")));
}

#[tokio::test]
async fn coerce_number_from_string() {
    let mut f = RecordStream::new("<freq:6>14.070<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    let num = rec.get("freq").unwrap().as_number().unwrap();
    assert_eq!(num, Decimal::from_str("14.070").unwrap());
}

#[tokio::test]
async fn coerce_date_from_string() {
    let mut f = RecordStream::new("<qso_date:8>20231215<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    let date = rec.get("qso_date").unwrap().as_date().unwrap();
    assert_eq!(date, NaiveDate::from_ymd_opt(2023, 12, 15).unwrap());
}

#[tokio::test]
async fn coerce_time_from_string() {
    let mut f = RecordStream::new("<time_on:6>143000<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    let time = rec.get("time_on").unwrap().as_time().unwrap();
    assert_eq!(time, NaiveTime::from_hms_opt(14, 30, 0).unwrap());
}

#[tokio::test]
async fn coerce_datetime_from_string() {
    let mut f =
        RecordStream::new("<dt:15>20240101 143000<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    let dt = rec.get("dt").unwrap().as_datetime().unwrap();
    let expected = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
    );
    assert_eq!(dt, expected);
}

#[tokio::test]
async fn coerce_invalid_number() {
    let mut f = RecordStream::new("<freq:7>invalid<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("freq").unwrap().as_number().is_none());
}

#[tokio::test]
async fn coerce_invalid_date() {
    let mut f = RecordStream::new("<qso_date:7>invalid<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("qso_date").unwrap().as_date().is_none());
}

#[tokio::test]
async fn coerce_invalid_time() {
    let mut f = RecordStream::new("<time_on:7>invalid<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("time_on").unwrap().as_time().is_none());
}

#[tokio::test]
async fn coerce_invalid_datetime() {
    let mut f = RecordStream::new("<dt:7>invalid<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("dt").unwrap().as_datetime().is_none());
}

#[tokio::test]
async fn boolean_y() {
    let mut f = RecordStream::new("<qsl:1:b>Y<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("qsl").unwrap().as_bool().unwrap());
    let mut f = RecordStream::new("<qsl:1:b>y<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("qsl").unwrap().as_bool().unwrap());
    let mut f = RecordStream::new("<qsl:1>Y<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(rec.get("qsl").unwrap().as_bool().unwrap());
}

#[tokio::test]
async fn boolean_n() {
    let mut f = RecordStream::new("<qsl:1>N<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(!rec.get("qsl").unwrap().as_bool().unwrap());
    let mut f = RecordStream::new("<qsl:1:b>N<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(!rec.get("qsl").unwrap().as_bool().unwrap());
    let mut f = RecordStream::new("<qsl:1:b>n<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert!(!rec.get("qsl").unwrap().as_bool().unwrap());
}

#[tokio::test]
async fn invalid_boolean() {
    let mut f = RecordStream::new("<qsl:1:b>X<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Owned("qsl:1:b".to_string())));
}

#[tokio::test]
async fn invalid_number() {
    let mut f = tags("<foo:3:n>abc");
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Owned("foo:3:n".to_string())));
}

#[tokio::test]
async fn invalid_date() {
    let mut f = tags("<qso_date:8:d>notadate");
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("qso_date:8:d".to_string()))
    );
}

#[tokio::test]
async fn invalid_time() {
    let mut f = tags("<time_on:6:t>notime");
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("time_on:6:t".to_string()))
    );
}

#[tokio::test]
async fn as_str_roundtrip() {
    let b = true;
    let datum = Datum::Boolean(b);
    let s = datum.as_str();
    assert_eq!(s, "Y");
    assert_eq!(Datum::String(s.to_string()).as_bool().unwrap(), b);

    let b = false;
    let datum = Datum::Boolean(b);
    let s = datum.as_str();
    assert_eq!(s, "N");
    assert_eq!(Datum::String(s.to_string()).as_bool().unwrap(), b);

    let n = Decimal::from_str("14.070").unwrap();
    let datum = Datum::Number(n);
    let s = datum.as_str();
    assert_eq!(Datum::String(s.to_string()).as_number().unwrap(), n);

    let d = NaiveDate::from_ymd_opt(2023, 12, 15).unwrap();
    let datum = Datum::Date(d);
    let s = datum.as_str();
    assert_eq!(s, "20231215");
    assert_eq!(Datum::String(s.to_string()).as_date().unwrap(), d);

    let t = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let datum = Datum::Time(t);
    let s = datum.as_str();
    assert_eq!(s, "143000");
    assert_eq!(Datum::String(s.to_string()).as_time().unwrap(), t);

    let dt = NaiveDateTime::new(d, t);
    let datum = Datum::DateTime(dt);
    let s = datum.as_str();
    assert_eq!(s, "20231215 143000");
    assert_eq!(Datum::String(s.to_string()).as_datetime().unwrap(), dt);

    let str = "hello world";
    let datum = Datum::String(str.to_string());
    let s = datum.as_str();
    assert_eq!(s, str);
}

#[tokio::test]
async fn case_insensitive_markers() {
    for variant in ["<EOH>", "<EoH>", "<Eoh>"] {
        let input = format!("<adifver:5>3.1.1 {variant}");
        let mut f = tags(&input);
        next_field(&mut f).await;
        next_eoh(&mut f).await;
        no_tags(&mut f).await;
    }

    for variant in ["<EOR>", "<EoR>", "<Eor>"] {
        let input = format!("<call:4>W1AW{variant}");
        let mut f = tags(&input).records();
        let rec = next_record(&mut f, false).await;
        assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
        no_records(&mut f).await;
    }

    let mut f =
        tags("<adifver:5>3.1.4 <EOH><call:4>W1AW<EOR><call:5>AB9BH<eor>")
            .records();
    let rec = next_record(&mut f, true).await;
    assert_eq!(rec.get("adifver").unwrap().as_str(), "3.1.4");
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "W1AW");
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "AB9BH");
    no_records(&mut f).await;
}

#[tokio::test]
async fn empty_field() {
    let mut f = RecordStream::new("<call:0><eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str(), "");
}

#[tokio::test]
async fn field_missing_length() {
    let mut f = RecordStream::new("<call:><eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Borrowed("call:")));
}

#[tokio::test]
async fn field_negative_length() {
    let mut f = RecordStream::new("<call:-1><eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Borrowed("call:-1")));
}

#[tokio::test]
async fn non_ascii_value() {
    // not sure now good of an idea this is, but it works
    let mut f = RecordStream::new("<name:6>André<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("name").unwrap().as_str(), "André");
}

#[tokio::test]
async fn invalid_utf8_in_field_value() {
    let bytes = b"<foo:2>\xFF\xFE<eor>";
    let mut f = RecordStream::new(bytes as &[u8], true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(err, Error::InvalidFormat(Cow::Owned("foo:2".to_string())));
}

#[tokio::test]
async fn invalid_utf8_in_tag_name() {
    let bytes = b"<\xFF\xFE:3>val<eor>";
    let mut f = RecordStream::new(bytes as &[u8], true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("\u{FFFD}\u{FFFD}:3".to_string()))
    );
}

#[tokio::test]
async fn invalid_utf8_in_tag_len() {
    let bytes = b"<foo:\xFF>val<eor>";
    let mut f = RecordStream::new(bytes as &[u8], true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("foo:\u{FFFD}".to_string()))
    );
}

#[tokio::test]
async fn invalid_utf8_in_type_spec() {
    let bytes = b"<foo:3:\xFF>val<eor>";
    let mut f = RecordStream::new(bytes as &[u8], true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("foo:3:\u{FFFD}".to_string()))
    );
}

#[tokio::test]
async fn duplicate_field() {
    let mut f =
        RecordStream::new("<call:4>W1AW<call:5>AB9BH<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    assert_eq!(
        err,
        Error::InvalidFormat(Cow::Owned("duplicate key: call".to_string()))
    );
}
