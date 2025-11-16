//! Optional ADIF data transformations

use crate::{Datum, Error, Record};
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
            res => res,
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
                res => return res,
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
pub fn normalize_times<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    stream.normalize(|record| {
        let date = record.get("qso_date").and_then(|d| d.as_date());
        let date_off = record.get("qso_date_off").and_then(|d| d.as_date());
        let time_on = record.get("time_on").and_then(|t| t.as_time());
        let time_off = record.get("time_off").and_then(|t| t.as_time());

        if let (Some(date), Some(time_on)) = (date, time_on) {
            let dt = NaiveDateTime::new(date, time_on);
            let _ = record.insert(":time_on".to_string(), Datum::DateTime(dt));

            if let Some(time_off) = time_off {
                let date = if let Some(date_off) = date_off {
                    date_off
                } else if time_off < time_on {
                    match date.checked_add_days(Days::new(1)) {
                        Some(d) => d,
                        None => return,
                    }
                } else {
                    date
                };
                let dt = NaiveDateTime::new(date, time_off);
                let _ =
                    record.insert(":time_off".to_string(), Datum::DateTime(dt));
            }
        }
    })
}

/// Normalize mode field from multiple possible source fields.
///
/// Coalesce mode from `mode`, `app_lotw_mode`, or `app_lotw_modegroup`
/// fields, and provide special handling for MFSK submodes (FT4, Q65).
pub fn normalize_mode<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    const MFSK_SUBMODES: &[&str] = &["FT4", "Q65"];

    stream.normalize(|record| {
        let mode = record
            .get("mode")
            .or_else(|| record.get("app_lotw_mode"))
            .or_else(|| record.get("app_lotw_modegroup"))
            .and_then(|m| m.as_str());

        let Some(mode) = mode else { return };
        let sub = record.get("submode").and_then(|s| s.as_str());

        let mode = match sub {
            Some(sub)
                if mode.eq_ignore_ascii_case("MFSK")
                    && MFSK_SUBMODES.contains(&sub.to_uppercase().as_str()) =>
            {
                sub
            }
            _ => mode,
        };

        let _ =
            record.insert(":mode".to_string(), Datum::String(mode.to_string()));
    })
}

/// Normalize band field to uppercase.
pub fn normalize_band<S>(
    stream: S,
) -> Normalize<S, impl FnMut(&mut Record) + Unpin>
where
    S: Stream<Item = Result<Record, Error>>,
{
    stream.normalize(|record| {
        let Some(band) = record.get("band").and_then(|b| b.as_str()) else {
            return;
        };

        let _ = record
            .insert(":band".to_string(), Datum::String(band.to_uppercase()));
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
        let Some(call) = record.get("call").and_then(|c| c.as_str()) else {
            return true;
        };
        !exclude.contains(&call.to_uppercase())
    })
}

/// Exclude header records from the stream.
pub fn exclude_header<S>(stream: S) -> Filter<S, impl FnMut(&Record) -> bool>
where
    S: Stream<Item = Result<Record, Error>>,
{
    stream.filter(|record| !record.is_header())
}
