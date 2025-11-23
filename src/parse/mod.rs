//! Parsing of ADIF data at various levels of sophistication

use crate::{Datum, Error, Field, Position, Record, Tag};
use bytes::{Buf, BytesMut};
use chrono::{NaiveDate, NaiveTime};
use futures::stream::Stream;
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, FramedRead};

#[cfg(test)]
mod test;

#[derive(Debug)]
enum ParserTag {
    Field(Field),
    Eoh,
    Eor,
    Eof,
}

/// Stream of ADIF tags from an async reader.
pub type TagStream<R> = FramedRead<R, TagDecoder>;

/// Decoder for parsing individual ADIF tags from a byte stream.
#[derive(Debug, Default)]
pub struct TagDecoder {
    ignore_partial: bool,
    consumed: usize,
    line: usize,
    column: usize,
}

impl TagDecoder {
    /// Create a new stream that returns ADIF tags.
    ///
    /// Tag names preserve their original case but are compared
    /// case-insensitively.
    ///
    /// If `ignore_partial` is `true`, incomplete trailing data will be
    /// silently ignored.  Set it to `false` to get an error in this
    /// situation.  Either way, trailing whitespace is silently consumed
    /// and will not return an error.
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::TagDecoder;
    /// use futures::StreamExt;
    /// let mut t = TagDecoder::new_stream("<FOO:3>123".as_bytes(), true);
    /// let tag = t.next().await.unwrap().unwrap();
    /// let field = tag.as_field().unwrap();
    /// assert_eq!(field.name(), "FOO");
    /// assert_eq!(field.value().as_str(), "123");
    /// # });
    /// ```
    pub fn new_stream<R>(reader: R, ignore_partial: bool) -> TagStream<R>
    where
        R: AsyncRead,
    {
        let decoder = Self {
            ignore_partial,
            consumed: 0,
            line: 1,
            column: 1,
        };
        FramedRead::new(reader, decoder)
    }

    fn position(&self) -> Position {
        Position {
            line: self.line,
            column: self.column,
            byte: self.consumed,
        }
    }

    fn invalid_tag(&self, tag: &[u8]) -> Error {
        Error::InvalidFormat {
            message: Cow::Owned(String::from_utf8_lossy(tag).into_owned()),
            position: self.position(),
        }
    }

    fn advance_slice(&mut self, data: &[u8]) {
        for &byte in data {
            if byte == b'\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
        }
        self.consumed += data.len();
    }

    fn advance(
        &mut self, src: &mut BytesMut, consumed: usize, skip_whitespace: bool,
    ) {
        self.advance_slice(&src[..consumed]);
        src.advance(consumed);

        // skip whitespace after a tag so "...<eor>\n" isn't an error, even
        // if ignore_partial is false
        if skip_whitespace {
            let whitespace = src
                .iter()
                .position(|&b| !b.is_ascii_whitespace())
                .unwrap_or(src.len());
            self.advance_slice(&src[..whitespace]);
            src.advance(whitespace);
        }
    }

    fn parse_typed_value(
        &self, tag: &[u8], v: &str, typ: Option<&str>,
    ) -> Result<Datum, Error> {
        match typ {
            Some("n") | Some("N") => {
                let num =
                    Decimal::from_str(v).map_err(|_| self.invalid_tag(tag))?;
                Ok(Datum::Number(num))
            }
            Some("b") | Some("B") => {
                let b = match v {
                    "Y" | "y" => true,
                    "N" | "n" => false,
                    _ => return Err(self.invalid_tag(tag)),
                };
                Ok(Datum::Boolean(b))
            }
            Some("d") | Some("D") => {
                let date = NaiveDate::parse_from_str(v, "%Y%m%d")
                    .map_err(|_| self.invalid_tag(tag))?;
                Ok(Datum::Date(date))
            }
            Some("t") | Some("T") => {
                let time = NaiveTime::parse_from_str(v, "%H%M%S")
                    .map_err(|_| self.invalid_tag(tag))?;
                Ok(Datum::Time(time))
            }
            _ => Ok(Datum::String(v.to_string())),
        }
    }

    fn as_str<'a>(&self, data: &'a [u8], tag: &[u8]) -> Result<&'a str, Error> {
        str::from_utf8(data).map_err(|_| self.invalid_tag(tag))
    }

    fn parse_value<'a>(
        &self, src: &'a BytesMut, offset: usize, tag: &'a [u8],
    ) -> Result<Option<(&'a str, Datum, usize)>, Error> {
        let err = || self.invalid_tag(tag);

        let mut parts = tag.split(|&b| b == b':');
        let (name, len, typ) =
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(name), Some(len), typ, None) => (name, len, typ),
                _ => return Err(err()),
            };

        let name = self.as_str(name, tag)?;
        let len = self.as_str(len, tag)?;
        let len = len.parse::<usize>().map_err(|_| err())?;
        let typ = typ.map(|t| self.as_str(t, tag)).transpose()?;

        let (begin, end) = (offset + 1, offset + 1 + len);
        if end > src.len() {
            return Ok(None);
        }

        let value = &src[begin..end];
        let value = self.as_str(value, tag)?;
        let value = self.parse_typed_value(tag, value, typ)?;

        Ok(Some((name, value, end)))
    }

    fn decode_inner(
        &mut self, src: &mut BytesMut,
    ) -> Result<Option<ParserTag>, Error> {
        let Some(begin) = src.iter().position(|&b| b == b'<') else {
            return Ok(None);
        };
        self.advance(src, begin, false);
        let Some(end) = src.iter().position(|&b| b == b'>') else {
            return Ok(None);
        };
        let tag = &src[1..end];

        if tag.eq_ignore_ascii_case(b"eoh") {
            let n = end + 1;
            self.advance(src, n, true);
            return Ok(Some(ParserTag::Eoh));
        } else if tag.eq_ignore_ascii_case(b"eor") {
            let n = end + 1;
            self.advance(src, n, true);
            return Ok(Some(ParserTag::Eor));
        } else if tag.eq_ignore_ascii_case(b"app_lotw_eof") {
            // ignore rest regardless of eof handling mode
            let n = src.len();
            self.advance(src, n, true);
            return Ok(Some(ParserTag::Eof));
        }

        let Some((name, value, end)) = self.parse_value(src, end, tag)? else {
            return Ok(None);
        };
        let tag = ParserTag::Field(Field::new(name, value));
        self.advance(src, end, true);

        Ok(Some(tag))
    }

    fn decode(
        &mut self, src: &mut BytesMut, eof: bool,
    ) -> Result<Option<Tag>, Error> {
        let res = self.decode_inner(src)?;
        let tag = match (res, eof, src.is_empty()) {
            (Some(tag), _, _) => tag, // return tag we got
            (None, false, _) => return Ok(None), // await more data
            (None, true, true) => return Ok(None), // at eof, nothing left
            (None, true, false) => {
                // at eof and eof handling was requested
                return Err(Error::InvalidFormat {
                    message: Cow::Borrowed("partial data at end of stream"),
                    position: self.position(),
                });
            }
        };
        let tag = match tag {
            ParserTag::Field(field) => Some(Tag::Field(field)),
            ParserTag::Eoh => Some(Tag::Eoh),
            ParserTag::Eor => Some(Tag::Eor),
            ParserTag::Eof => None,
        };
        Ok(tag)
    }
}

impl Decoder for TagDecoder {
    type Item = Tag;
    type Error = Error;

    fn decode(
        &mut self, src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src, false)
    }

    fn decode_eof(
        &mut self, src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(src, !self.ignore_partial)
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
            record: Record::new(),
        }
    }
}

impl<S> RecordStreamExt for S where S: Stream {}

/// Stream that aggregates ADIF tags into complete records.
pub struct RecordStream<S> {
    stream: S,
    record: Record,
}

impl<S> RecordStream<S> {
    fn make(&mut self, header: bool) -> Poll<Option<Result<Record, Error>>> {
        let mut record = std::mem::take(&mut self.record);
        record.header = header;
        Poll::Ready(Some(Ok(record)))
    }
}

impl<R> RecordStream<TagStream<R>>
where
    R: AsyncRead,
{
    /// Create a new stream that returns ADIF records.
    ///
    /// Tag names preserve their original case but are compared
    /// case-insensitively.
    ///
    /// If `ignore_partial` is `true`, incomplete trailing data will be
    /// silently ignored.  Set it to `false` to get an error in this
    /// situation.  Either way, trailing whitespace is silently consumed
    /// and will not return an error.
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::RecordStream;
    /// use futures::StreamExt;
    /// let mut r = RecordStream::new("<FOO:3>123<eor>".as_bytes(), true);
    /// let rec = r.next().await.unwrap().unwrap();
    /// assert_eq!(rec.get("foo").unwrap().as_number().unwrap(), 123.into());
    /// assert_eq!(rec.get("FOO").unwrap().as_number().unwrap(), 123.into());
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
                    if let Err(e) = self.record.insert(field.name, field.value)
                    {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
