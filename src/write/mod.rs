//! Writing ADIF data to async writers

use crate::{Datum, Error, Record, Tag};
use bytes::{BufMut, BytesMut};
use futures::sink::Sink;
use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio_util::codec::{Encoder, FramedWrite};

#[cfg(test)]
mod test;

/// Configuration for type specifier output
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputTypes {
    /// Always include type specifiers for all fields
    Always,
    /// Only include type specifiers for non-string types
    OnlyNonString,
    /// Never include type specifiers
    #[default]
    Never,
}

/// Encoder for writing individual ADIF tags to a byte stream
#[derive(Debug, Default)]
pub struct TagEncoder {
    types: OutputTypes,
}

impl TagEncoder {
    /// Create a new TagEncoder with default configuration.
    ///
    /// ```
    /// use adif::{Field, Tag, TagEncoder};
    /// use bytes::BytesMut;
    /// use tokio_util::codec::Encoder;
    ///
    /// let mut encoder = TagEncoder::new();
    /// let mut buf = BytesMut::new();
    /// let field = Field::new("call", "W1AW");
    /// encoder.encode(Tag::Field(field), &mut buf).unwrap();
    /// assert_eq!(&buf[..], b"<call:4>W1AW");
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new TagEncoder with specified type specifier behavior.
    ///
    /// ```
    /// use adif::{Field, OutputTypes, Tag, TagEncoder};
    /// use bytes::BytesMut;
    /// use tokio_util::codec::Encoder;
    ///
    /// let mut encoder = TagEncoder::with_types(OutputTypes::Always);
    /// let mut buf = BytesMut::new();
    /// let field = Field::new("call", "W1AW");
    /// encoder.encode(Tag::Field(field), &mut buf).unwrap();
    /// assert_eq!(&buf[..], b"<call:4:s>W1AW");
    /// ```
    pub fn with_types(types: OutputTypes) -> Self {
        Self { types }
    }

    /// Create a sink from this encoder and a writer.
    ///
    /// ```
    /// use adif::{Field, Tag, TagEncoder};
    /// use futures::SinkExt;
    ///
    /// # tokio_test::block_on(async {
    /// let mut buf = Vec::new();
    /// let mut sink = TagEncoder::new().tag_sink_with(&mut buf);
    /// let field = Field::new("call", "W1AW");
    /// sink.send(Tag::Field(field)).await.unwrap();
    /// sink.close().await.unwrap();
    /// assert_eq!(buf, b"<call:4>W1AW");
    /// # })
    /// ```
    pub fn tag_sink_with<W>(self, writer: W) -> TagSink<W>
    where
        W: AsyncWrite,
    {
        FramedWrite::new(writer, self)
    }

    fn type_indicator(
        &self, datum: &Datum,
    ) -> Result<Option<&'static str>, Error> {
        match (self.types, datum) {
            (_, Datum::DateTime(_)) => {
                Err(Error::InvalidFormat(Cow::Borrowed(
                    "DateTime cannot be output directly; split into date \
                     and time fields",
                )))
            }
            (OutputTypes::Never, _) => Ok(None),
            (_, Datum::Boolean(_)) => Ok(Some("b")),
            (_, Datum::Number(_)) => Ok(Some("n")),
            (_, Datum::Date(_)) => Ok(Some("d")),
            (_, Datum::Time(_)) => Ok(Some("t")),
            (OutputTypes::Always, Datum::String(_)) => Ok(Some("s")),
            (_, Datum::String(_)) => Ok(None),
        }
    }

    fn encode_eoh(dst: &mut BytesMut) {
        dst.put_slice(b"<eoh>\n");
    }

    fn encode_eor(dst: &mut BytesMut) {
        dst.put_slice(b"<eor>\n");
    }

    fn encode_field(
        &self, name: &str, value: &Datum, dst: &mut BytesMut,
    ) -> Result<(), Error> {
        let s = value.as_str();

        dst.put_u8(b'<');
        dst.put_slice(name.as_bytes());
        dst.put_u8(b':');
        let mut buf = itoa::Buffer::new();
        dst.put_slice(buf.format(s.len()).as_bytes());
        if let Some(typ) = self.type_indicator(value)? {
            dst.put_u8(b':');
            dst.put_slice(typ.as_bytes());
        }
        dst.put_u8(b'>');
        dst.put_slice(s.as_bytes());
        Ok(())
    }
}

impl Encoder<Tag> for TagEncoder {
    type Error = Error;

    fn encode(
        &mut self, item: Tag, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            Tag::Eoh => Self::encode_eoh(dst),
            Tag::Eor => Self::encode_eor(dst),
            Tag::Field(field) => {
                self.encode_field(field.name(), field.value(), dst)?;
            }
        }
        Ok(())
    }
}

/// Internal tag type for writing with borrowed field data
enum WriterTag<'a> {
    Field { name: &'a str, value: &'a Datum },
    Eoh,
    Eor,
}

/// Wrapper around TagEncoder for encoding WriterTag
struct WriterTagEncoder(TagEncoder);

impl Encoder<WriterTag<'_>> for WriterTagEncoder {
    type Error = Error;

    fn encode(
        &mut self, item: WriterTag<'_>, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            WriterTag::Eoh => TagEncoder::encode_eoh(dst),
            WriterTag::Eor => TagEncoder::encode_eor(dst),
            WriterTag::Field { name, value } => {
                self.0.encode_field(name, value, dst)?;
            }
        }
        Ok(())
    }
}

/// Stream of ADIF tags to an async writer
pub type TagSink<W> = FramedWrite<W, TagEncoder>;

/// Extension trait for creating tag sinks
pub trait TagSinkExt: AsyncWrite + Sized {
    /// Create a new sink that writes ADIF tags
    fn tag_sink(self) -> TagSink<Self> {
        TagEncoder::new().tag_sink_with(self)
    }

    /// Create a new sink with specified type specifier behavior
    fn tag_sink_with_types(self, types: OutputTypes) -> TagSink<Self> {
        TagEncoder::with_types(types).tag_sink_with(self)
    }
}

impl<W> TagSinkExt for W where W: AsyncWrite {}

/// Sink for writing ADIF records to an async writer
pub struct RecordSink<W> {
    inner: FramedWrite<W, WriterTagEncoder>,
}

impl<W> RecordSink<W>
where
    W: AsyncWrite + Unpin,
{
    /// Create a new RecordSink with default configuration.
    ///
    /// ```
    /// use adif::{Record, RecordSink};
    /// use futures::SinkExt;
    ///
    /// # tokio_test::block_on(async {
    /// let mut buf = Vec::new();
    /// let mut sink = RecordSink::new(&mut buf);
    ///
    /// let mut record = Record::new();
    /// record.insert("call", "W1AW").unwrap();
    /// sink.send(record).await.unwrap();
    /// sink.close().await.unwrap();
    ///
    /// assert_eq!(buf, b"<call:4>W1AW<eor>\n");
    /// # })
    /// ```
    pub fn new(writer: W) -> Self {
        Self {
            inner: FramedWrite::new(
                writer,
                WriterTagEncoder(TagEncoder::new()),
            ),
        }
    }

    /// Create a new RecordSink with given type specifier behavior.
    pub fn with_types(writer: W, types: OutputTypes) -> Self {
        Self {
            inner: FramedWrite::new(
                writer,
                WriterTagEncoder(TagEncoder::with_types(types)),
            ),
        }
    }
}

impl<W> Sink<Record> for RecordSink<W>
where
    W: AsyncWrite + Unpin,
{
    type Error = Error;

    fn poll_ready(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>, item: Record,
    ) -> Result<(), Self::Error> {
        let tag = if item.is_header() {
            WriterTag::Eoh
        } else {
            WriterTag::Eor
        };
        for (name, value) in item.fields() {
            Pin::new(&mut self.inner)
                .start_send(WriterTag::Field { name, value })?;
        }

        Pin::new(&mut self.inner).start_send(tag)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
