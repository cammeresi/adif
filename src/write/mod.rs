//! Writing ADIF data to async writers

use crate::{Datum, Error, Field, Record, Tag};
use bytes::{BufMut, BytesMut};
use futures::sink::Sink;
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new TagEncoder with specified type specifier behavior.
    pub fn with_types(types: OutputTypes) -> Self {
        Self { types }
    }

    fn type_indicator(&self, datum: &Datum) -> Option<&'static str> {
        match (self.types, datum) {
            (OutputTypes::Never, _) => None,
            (_, Datum::Boolean(_)) => Some("b"),
            (_, Datum::Number(_)) => Some("n"),
            (_, Datum::Date(_)) => Some("d"),
            (_, Datum::Time(_)) => Some("t"),
            (_, Datum::DateTime(_)) => None, // shouldn't happen
            (OutputTypes::Always, Datum::String(_)) => Some("s"),
            (_, Datum::String(_)) => None,
        }
    }
}

impl Encoder<Tag> for TagEncoder {
    type Error = Error;

    fn encode(
        &mut self, item: Tag, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            Tag::Eoh => {
                dst.put_slice(b"<eoh>\n");
            }
            Tag::Eor => {
                dst.put_slice(b"<eor>\n");
            }
            Tag::Field(field) => {
                if matches!(field.value(), Datum::DateTime(_)) {
                    return Err(Error::InvalidFormat(
                        "DateTime cannot be output directly; \
                         split into date and time fields"
                            .to_string(),
                    ));
                }

                let value = field.value().as_str().ok_or_else(|| {
                    let e = "Cannot convert value to string".to_string();
                    Error::InvalidFormat(e)
                })?;

                dst.put_u8(b'<');
                dst.put_slice(field.name().as_bytes());
                dst.put_u8(b':');
                dst.put_slice(value.len().to_string().as_bytes());
                if let Some(typ) = self.type_indicator(field.value()) {
                    dst.put_u8(b':');
                    dst.put_slice(typ.as_bytes());
                }
                dst.put_u8(b'>');
                dst.put_slice(value.as_bytes());
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

impl TagEncoder {
    /// Create a sink from this encoder and a writer.
    pub fn tag_sink_with<W>(self, writer: W) -> TagSink<W>
    where
        W: AsyncWrite,
    {
        FramedWrite::new(writer, self)
    }
}

/// Sink for writing ADIF records to an async writer
pub struct RecordSink<W> {
    inner: TagSink<W>,
}

impl<W> RecordSink<W>
where
    W: AsyncWrite + Unpin,
{
    /// Create a new RecordSink with default configuration.
    pub fn new(writer: W) -> Self {
        Self {
            inner: TagEncoder::new().tag_sink_with(writer),
        }
    }

    /// Create a new RecordSink with specified type specifier behavior.
    pub fn with_types(writer: W, types: OutputTypes) -> Self {
        Self {
            inner: TagEncoder::with_types(types).tag_sink_with(writer),
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
        for (name, value) in item.fields() {
            let field = Field::new(name.clone(), value.clone());
            Pin::new(&mut self.inner).start_send(Tag::Field(field))?;
        }

        let tag = if item.is_header() { Tag::Eoh } else { Tag::Eor };
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
