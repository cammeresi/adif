//! Optional ADIF data transformations

use crate::{Error, Record};
use chrono::{Days, NaiveDateTime};
use futures::stream::Stream;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(test)]
mod test;

/// Stream adapter that applies an in-place transformation to each record.
pub struct Normalize<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Stream for Normalize<S, F>
where
    S: Stream<Item = Result<Record, Error>> + Unpin,
    F: FnMut(&mut Record) + Unpin,
{
    type Item = Result<Record, Error>;

    fn poll_next(
        self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(mut record))) => {
                (this.f)(&mut record);
                Poll::Ready(Some(Ok(record)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Extension trait providing the `normalize` method on streams.
pub trait NormalizeExt: Stream {
    /// Apply an in-place transformation to each record in the stream.
    fn normalize<F>(self, f: F) -> Normalize<Self, F>
    where
        Self: Sized,
        F: FnMut(&mut Record),
    {
        Normalize { stream: self, f }
    }
}

impl<S> NormalizeExt for S where S: Stream {}

/// Stream adapter that yields or removes records based on a predicate.
pub struct Filter<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Stream for Filter<S, F>
where
    S: Stream<Item = Result<Record, Error>> + Unpin,
    F: FnMut(&Record) -> bool + Unpin,
{
    type Item = Result<Record, Error>;

    fn poll_next(
        self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(record))) => {
                    if (this.f)(&record) {
                        return Poll::Ready(Some(Ok(record)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Extension trait providing the `filter` method on streams.
pub trait FilterExt: Stream {
    /// Filter records, yielding only those for which the predicate is true.
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        Self: Sized,
        F: FnMut(&Record) -> bool,
    {
        Filter { stream: self, f }
    }
}

impl<S> FilterExt for S where S: Stream {}

/// Normalize date and time fields from multiple possible source fields into
/// combined datetime values.
///
/// Create `:time_on` and `:time_off` fields from separate date/time
/// components.  Handle date crossing when time_off is earlier than time_on.
///
/// ```
/// use adif::filter::normalize_times;
/// use adif::{Datum, Record, RecordStreamExt, TagDecoder};
/// use chrono::{NaiveDate, NaiveTime, Timelike};
/// use futures::StreamExt;
///
/// # tokio_test::block_on(async {
/// let data = b"<qso_date:8>20240101<time_on:6>230000<eor>";
/// let stream = TagDecoder::new_stream(&data[..], true).records();
/// let mut stream = normalize_times(stream);
/// let record = stream.next().await.unwrap().unwrap();
/// let dt = record
///     .get(":time_on")
///     .and_then(|d| d.as_datetime())
///     .unwrap();
/// assert_eq!(dt.date(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
/// assert_eq!(dt.time(), NaiveTime::from_hms_opt(23, 0, 0).unwrap());
/// # });
/// ```
pub fn normalize_times<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    const TIME_ON: &str = ":time_on";
    const TIME_OFF: &str = ":time_off";

    stream.normalize(|record| {
        let date = record.get("qso_date").and_then(|d| d.as_date());
        let date_off = record.get("qso_date_off").and_then(|d| d.as_date());
        let time_on = record.get("time_on").and_then(|t| t.as_time());
        let time_off = record.get("time_off").and_then(|t| t.as_time());

        if let (Some(date), Some(time_on)) = (date, time_on) {
            let dt = NaiveDateTime::new(date, time_on);
            let _ = record.insert(TIME_ON, dt);

            if let Some(time_off) = time_off {
                let date = if let Some(date_off) = date_off {
                    date_off
                } else if time_off < time_on {
                    date + Days::new(1)
                } else {
                    date
                };
                let dt = NaiveDateTime::new(date, time_off);
                let _ = record.insert(TIME_OFF, dt);
            }
        }
    })
}

/// Normalize mode field from multiple possible source fields.
///
/// Coalesce mode from `mode`, `app_lotw_mode`, or `app_lotw_modegroup`
/// fields, and promote to full modes the modes that LOTW considers submodes
/// of MFSK (i.e. FT4, Q65).
pub fn normalize_mode<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    const MFSK_SUBMODES: &[&str] = &["FT4", "Q65"];
    const MODE: &str = ":mode";

    stream.normalize(|record| {
        let mode = record
            .get("mode")
            .or_else(|| record.get("app_lotw_mode"))
            .or_else(|| record.get("app_lotw_modegroup"))
            .map(|m| m.as_str());

        let Some(mode) = mode else { return };
        let sub = record.get("submode").map(|s| s.as_str());

        let mode = match sub {
            Some(sub)
                if mode.eq_ignore_ascii_case("MFSK")
                    && MFSK_SUBMODES
                        .iter()
                        .any(|m| m.eq_ignore_ascii_case(&sub)) =>
            {
                sub
            }
            _ => mode,
        };

        let _ = record.insert(MODE, mode.into_owned());
    })
}

/// Normalize band field to uppercase.
///
/// ```
/// use adif::{
///     Record, RecordStreamExt, TagDecoder, filter::normalize_band,
/// };
/// use futures::StreamExt;
///
/// # tokio_test::block_on(async {
/// let data = b"<band:3>20m<eor>";
/// let stream = TagDecoder::new_stream(&data[..], true).records();
/// let mut stream = normalize_band(stream);
/// let record = stream.next().await.unwrap().unwrap();
/// let band = record.get(":band").map(|b| b.as_str()).unwrap();
/// assert_eq!(band, "20M");
/// # });
/// ```
pub fn normalize_band<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    const BAND: &str = ":band";

    stream.normalize(|record| {
        let Some(band) = record.get("band").map(|b| b.as_str()) else {
            return;
        };

        let band =
            if band.chars().all(|c| c.is_uppercase() || !c.is_alphabetic()) {
                band.to_string()
            } else {
                band.to_uppercase()
            };
        let _ = record.insert(BAND, band);
    })
}

/// Exclude records matching specified callsigns.
///
/// Case-insensitive comparison.  Records without a `call` field pass through.
pub fn exclude_callsigns<S>(
    stream: S, callsigns: &[&str],
) -> Filter<S, impl FnMut(&Record) -> bool>
where
    S: Stream<Item = Result<Record, Error>>,
{
    let exclude: HashSet<String> =
        callsigns.iter().map(|c| c.to_uppercase()).collect();

    stream.filter(move |record| {
        let Some(call) = record.get("call").map(|c| c.as_str()) else {
            return true;
        };
        !exclude.iter().any(|e| e.eq_ignore_ascii_case(&call))
    })
}

/// Exclude header records from the stream.
///
/// ```
/// use adif::{
///     Record, RecordStreamExt, TagDecoder, filter::exclude_header,
/// };
/// use futures::StreamExt;
///
/// # tokio_test::block_on(async {
/// let data = b"<foo:3>bar<eoh><call:4>W1AW<eor>";
/// let stream = TagDecoder::new_stream(&data[..], true).records();
/// let mut stream = exclude_header(stream);
/// let record = stream.next().await.unwrap().unwrap();
/// assert!(!record.is_header());
/// assert_eq!(record.get("call").unwrap().as_str(), "W1AW");
/// # });
/// ```
pub fn exclude_header<S>(stream: S) -> Filter<S, impl FnMut(&Record) -> bool>
where
    S: Stream<Item = Result<Record, Error>>,
{
    stream.filter(|record| !record.is_header())
}
