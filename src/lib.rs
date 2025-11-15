//#![warn(missing_docs)]

use bytes::{Buf, BytesMut};
use chrono::{NaiveDate, NaiveTime};
use futures::stream::Stream;
use rust_decimal::Decimal;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, FramedRead};

#[cfg(test)]
mod test;

pub type TagStream<R> = FramedRead<R, TagDecoder>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid ADIF format: {0}")]
    InvalidFormat(String),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::Io(a), Error::Io(b)) => a.kind() == b.kind(),
            (Error::InvalidFormat(a), Error::InvalidFormat(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Data {
    Boolean(bool),
    Number(Decimal),
    Date(NaiveDate),
    Time(NaiveTime),
    String(String),
}

impl Data {
    pub fn as_bool(&self) -> Option<bool> {
        let Self::Boolean(b) = self else {
            return None;
        };
        Some(*b)
    }

    pub fn as_number(&self) -> Option<Decimal> {
        let Self::Number(n) = self else {
            return None;
        };
        Some(*n)
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        let Self::Date(d) = self else {
            return None;
        };
        Some(*d)
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        let Self::Time(t) = self else {
            return None;
        };
        Some(*t)
    }

    pub fn as_str(&self) -> Option<&str> {
        let Self::String(s) = self else {
            return None;
        };
        Some(s)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    name: String,
    value: Data,
}

impl Field {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &Data {
        &self.value
    }
}

#[derive(Debug, PartialEq, Eq)]
/// A single tag and following value within an ADIF stream
pub enum Tag {
    /// A data field with name and value
    Field(Field),
    /// End of header
    Eoh,
    /// End of record
    Eor,
}

impl Tag {
    /// Returns `Some` if this is a `Field` tag, otherwise `None`.
    pub fn as_field(&self) -> Option<&Field> {
        let Tag::Field(field) = self else {
            return None;
        };
        Some(field)
    }

    /// Returns `true` if this is an end-of-header tag.
    pub fn is_eoh(&self) -> bool {
        matches!(self, Tag::Eoh)
    }

    /// Returns `true` if this is an end-of-record tag.
    pub fn is_eor(&self) -> bool {
        matches!(self, Tag::Eor)
    }
}

#[derive(Debug, Default)]
pub struct TagDecoder {
    ignore_partial: bool,
}

impl TagDecoder {
    /// Create a new stream that returns ADIF tags.
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::TagDecoder;
    /// use futures::StreamExt;
    /// let mut t = TagDecoder::new_stream("<foo:3>123".as_bytes(), true);
    /// let tag = t.next().await.unwrap().unwrap();
    /// assert!(tag.as_field().is_some());
    /// # });
    /// ```
    pub fn new_stream<R>(reader: R, ignore_partial: bool) -> TagStream<R>
    where
        R: AsyncRead,
    {
        FramedRead::new(reader, Self { ignore_partial })
    }

    fn invalid_tag(tag: &[u8]) -> Error {
        Error::InvalidFormat(String::from_utf8_lossy(tag).to_string())
    }

    fn parse_value(
        tag: &[u8], end: usize, src: &BytesMut,
    ) -> Result<Option<(String, Data, usize)>, Error> {
        let parts: Vec<&[u8]> = tag.split(|&b| b == b':').collect();
        let (name, len, typ) = match parts[..] {
            [name, len] => (name, len, None),
            [name, len, typ] => (name, len, Some(typ)),
            _ => {
                return Err(Self::invalid_tag(tag));
            }
        };

        let name = String::from_utf8_lossy(name).to_lowercase();
        let len = String::from_utf8_lossy(len)
            .parse::<usize>()
            .map_err(|_| Self::invalid_tag(tag))?;
        let typ = typ.map(|t| String::from_utf8_lossy(t).to_lowercase());
        let (begin, end) = (end + 1, end + 1 + len);
        if end > src.len() {
            return Ok(None);
        }

        let s = String::from_utf8_lossy(&src[begin..end]);
        let value = match typ.as_deref() {
            Some("n") => {
                let num = Decimal::from_str(&s)
                    .map_err(|_| Self::invalid_tag(tag))?;
                Data::Number(num)
            }
            Some("b") => todo!(),
            Some("d") => todo!(),
            Some("t") => todo!(),
            _ => Data::String(s.to_string()),
        };

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

        let Some(end) = src[begin..].iter().position(|&b| b == b'>') else {
            return Ok(None);
        };

        let (begin, end) = (begin + 1, begin + end);
        let tag = &src[begin..end];

        if tag == b"eoh" {
            src.advance(end + 1);
            return Ok(Some(Tag::Eoh));
        } else if tag == b"eor" {
            src.advance(end + 1);
            return Ok(Some(Tag::Eor));
        }

        let Some((name, value, end)) = Self::parse_value(tag, end, src)? else {
            return Ok(None);
        };
        src.advance(end);

        Ok(Some(Tag::Field(Field { name, value })))
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
                    Err(Error::InvalidFormat(
                        "partial data at end of stream".to_string(),
                    ))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    header: bool,
    fields: HashMap<String, Data>,
}

impl Record {
    pub fn is_header(&self) -> bool {
        self.header
    }

    pub fn get<Q>(&self, name: &Q) -> Option<&Data>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fields.get(name)
    }

    pub fn get_mut<Q>(&mut self, name: &Q) -> Option<&mut Data>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fields.get_mut(name)
    }

    pub fn insert(&mut self, name: String, value: Data) -> Option<Data> {
        self.fields.insert(name, value)
    }

    pub fn remove<Q>(&mut self, name: &Q) -> Option<Data>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fields.remove(name)
    }
}

pub trait RecordStreamExt: Stream {
    fn records(self) -> RecordStream<Self>
    where
        Self: Sized,
    {
        RecordStream {
            stream: self,
            fields: HashMap::new(),
        }
    }
}

impl<S> RecordStreamExt for S where S: Stream {}

pub struct RecordStream<S> {
    stream: S,
    fields: HashMap<String, Data>,
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
