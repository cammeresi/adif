use crate::{Data, Error, Record};
use chrono::{Days, NaiveDateTime};
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(test)]
mod test;

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

pub trait NormalizeExt: Stream {
    fn normalize<F>(self, f: F) -> Normalize<Self, F>
    where
        Self: Sized,
        F: FnMut(&mut Record),
    {
        Normalize { stream: self, f }
    }
}

impl<S> NormalizeExt for S where S: Stream {}

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
            let _ = record.insert(":time_on".to_string(), Data::DateTime(dt));

            if let Some(time_off) = time_off {
                let date = if let Some(date_off) = date_off {
                    date_off
                } else if time_off < time_on {
                    date.checked_add_days(Days::new(1)).unwrap()
                } else {
                    date
                };
                let dt = NaiveDateTime::new(date, time_off);
                let _ =
                    record.insert(":time_off".to_string(), Data::DateTime(dt));
            }
        }
    })
}

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
            record.insert(":mode".to_string(), Data::String(mode.to_string()));
    })
}
