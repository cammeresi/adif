use crate::{Data, Error, Record};
use chrono::{Days, NaiveDateTime};
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    stream.normalize(|record| {
        let mode = record
            .get("mode")
            .or_else(|| record.get("app_lotw_mode"))
            .or_else(|| record.get("app_lotw_modegroup"))
            .and_then(|m| m.as_str());

        if let Some(m) = mode {
            let _ =
                record.insert(":mode".to_string(), Data::String(m.to_string()));
        }
    })
}

#[cfg(test)]
mod test {
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
        let stream = RecordStream::new(
            "<app_lotw_modegroup:4>RTTY<eor>".as_bytes(),
            true,
        );
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
}
