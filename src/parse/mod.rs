//! Parsing of ADIF data at various levels of sophistication

use crate::{Datum, Error, Field, Record, Tag};
use bytes::{Buf, BytesMut};
use chrono::{NaiveDate, NaiveTime};
use futures::stream::Stream;
use indexmap::IndexMap;
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, FramedRead};

#[cfg(test)]
mod test;

/// Stream of ADIF tags from an async reader.
pub type TagStream<R> = FramedRead<R, TagDecoder>;

/// Decoder for parsing individual ADIF tags from a byte stream.
#[derive(Debug, Default)]
pub struct TagDecoder {
    ignore_partial: bool,
}

impl TagDecoder {
    /// Create a new stream that returns ADIF tags.
    ///
    /// Tag names are converted to lowercase.
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::TagDecoder;
    /// use futures::StreamExt;
    /// let mut t = TagDecoder::new_stream("<FOO:3>123".as_bytes(), true);
    /// let tag = t.next().await.unwrap().unwrap();
    /// let field = tag.as_field().unwrap();
    /// assert_eq!(field.name(), "foo");
    /// assert_eq!(field.value().as_str().unwrap(), "123");
    /// # });
    /// ```
    pub fn new_stream<R>(reader: R, ignore_partial: bool) -> TagStream<R>
    where
        R: AsyncRead,
    {
        FramedRead::new(reader, Self { ignore_partial })
    }

    fn invalid_tag(tag: &[u8]) -> Error {
        Error::InvalidFormat(Cow::Owned(
            String::from_utf8_lossy(tag).into_owned(),
        ))
    }

    fn parse_typed_value(
        typ: Option<&str>, s: &str, tag: &[u8],
    ) -> Result<Datum, Error> {
        match typ {
            Some("n") | Some("N") => {
                let num =
                    Decimal::from_str(s).map_err(|_| Self::invalid_tag(tag))?;
                Ok(Datum::Number(num))
            }
            Some("b") | Some("B") => {
                let b = match s {
                    "Y" | "y" => true,
                    "N" | "n" => false,
                    _ => return Err(Self::invalid_tag(tag)),
                };
                Ok(Datum::Boolean(b))
            }
            Some("d") | Some("D") => {
                let date = NaiveDate::parse_from_str(s, "%Y%m%d")
                    .map_err(|_| Self::invalid_tag(tag))?;
                Ok(Datum::Date(date))
            }
            Some("t") | Some("T") => {
                let time = NaiveTime::parse_from_str(s, "%H%M%S")
                    .map_err(|_| Self::invalid_tag(tag))?;
                Ok(Datum::Time(time))
            }
            _ => Ok(Datum::String(s.to_string())),
        }
    }

    fn parse_value(
        tag: &[u8], end: usize, src: &BytesMut,
    ) -> Result<Option<(String, Datum, usize)>, Error> {
        let err = || Self::invalid_tag(tag);

        let mut parts = tag.split(|&b| b == b':');
        let (name, len, typ) =
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(name), Some(len), None, None) => (name, len, None),
                (Some(name), Some(len), Some(typ), None) => {
                    (name, len, Some(typ))
                }
                _ => return Err(err()),
            };

        let name = str::from_utf8(name)
            .map_err(|_| err())?
            .to_ascii_lowercase();
        let len = str::from_utf8(len).map_err(|_| err())?;
        let len = len.parse::<usize>().map_err(|_| err())?;
        let typ = typ
            .map(|t| str::from_utf8(t))
            .transpose()
            .map_err(|_| err())?;

        let (begin, end) = (end + 1, end + 1 + len);
        if end > src.len() {
            return Ok(None);
        }

        let Some(s) = src.get(begin..end) else {
            return Ok(None); // shouldn't happen
        };
        let s = str::from_utf8(s).map_err(|_| err())?;
        let value = Self::parse_typed_value(typ, s, tag)?;

        Ok(Some((name, value, end)))
    }
}

impl Decoder for TagDecoder {
    type Item = Tag;
    type Error = Error;

    fn decode(
        &mut self, src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let Some(begin) = src.iter().position(|&b| b == b'<') else {
            src.clear();
            return Ok(None);
        };

        let Some(remainder) = src.get(begin..) else {
            return Ok(None); // shouldn't happen
        };
        let Some(end) = remainder.iter().position(|&b| b == b'>') else {
            return Ok(None); // shouldn't happen
        };

        let (begin, end) = (begin + 1, begin + end);
        let Some(tag) = src.get(begin..end) else {
            return Ok(None); // shouldn't happen
        };

        if tag.eq_ignore_ascii_case(b"eoh") {
            src.advance(end + 1);
            return Ok(Some(Tag::Eoh));
        } else if tag.eq_ignore_ascii_case(b"eor") {
            src.advance(end + 1);
            return Ok(Some(Tag::Eor));
        } else if tag.eq_ignore_ascii_case(b"app_lotw_eof") {
            src.clear();
            return Ok(None);
        }

        let Some((name, value, end)) = Self::parse_value(tag, end, src)? else {
            return Ok(None);
        };
        src.advance(end);

        Ok(Some(Tag::Field(Field::new(name, value))))
    }

    fn decode_eof(
        &mut self, src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(src)? {
            Some(item) => Ok(Some(item)),
            None => {
                if self.ignore_partial {
                    src.clear();
                    Ok(None)
                } else if !src.is_empty() {
                    Err(Error::InvalidFormat(Cow::Borrowed(
                        "partial data at end of stream",
                    )))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// Extension trait providing the `records` method on tag streams.
pub trait RecordStreamExt: Stream {
    /// Aggregate tags into records.
    fn records(self) -> RecordStream<Self>
    where
        Self: Sized,
    {
        RecordStream {
            stream: self,
            fields: IndexMap::new(),
        }
    }
}

impl<S> RecordStreamExt for S where S: Stream {}

/// Stream that aggregates ADIF tags into complete records.
pub struct RecordStream<S> {
    stream: S,
    fields: IndexMap<String, Datum>,
}

impl<S> RecordStream<S> {
    fn make(&mut self, header: bool) -> Poll<Option<Result<Record, Error>>> {
        let fields = std::mem::take(&mut self.fields);
        Poll::Ready(Some(Ok(Record { header, fields })))
    }
}

impl<R> RecordStream<TagStream<R>>
where
    R: AsyncRead,
{
    /// Create a new stream that returns ADIF records.
    ///
    /// Tag names in the returned records are converted to lowercase.
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::RecordStream;
    /// use futures::StreamExt;
    /// let mut r = RecordStream::new("<FOO:3>123<eor>".as_bytes(), true);
    /// let rec = r.next().await.unwrap().unwrap();
    /// assert_eq!(rec.get("foo").unwrap().as_number().unwrap(), 123.into());
    /// # });
    /// ```
    pub fn new(reader: R, ignore_partial: bool) -> Self {
        TagDecoder::new_stream(reader, ignore_partial).records()
    }
}

impl<S> Stream for RecordStream<S>
where
    S: Stream<Item = Result<Tag, Error>> + Unpin,
{
    type Item = Result<Record, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(Tag::Eoh))) => return self.make(true),
                Poll::Ready(Some(Ok(Tag::Eor))) => return self.make(false),
                Poll::Ready(Some(Ok(Tag::Field(field)))) => {
                    self.fields.insert(field.name, field.value);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
