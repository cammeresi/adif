use std::borrow::Cow;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::io::{AsyncRead, ReadBuf};

use crate::{Error, Position, Record};

pub(crate) fn invalid_format(
    message: &'static str, line: usize, column: usize, byte: usize,
) -> Error {
    Error::InvalidFormat {
        message: Cow::Borrowed(message),
        position: Position { line, column, byte },
    }
}

pub(crate) fn partial_data(line: usize, column: usize, byte: usize) -> Error {
    invalid_format("partial data at end of stream", line, column, byte)
}

pub(crate) fn duplicate_key(key: &str, record: Record) -> Error {
    Error::DuplicateKey {
        key: key.to_string(),
        record,
    }
}

pub(crate) fn cannot_output(typ: &'static str, reason: &'static str) -> Error {
    Error::CannotOutput { typ, reason }
}

pub(crate) struct TrickleReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
    delayed: bool,
}

impl TrickleReader {
    pub(crate) fn new(data: &str, chunk: usize) -> Self {
        Self {
            data: data.as_bytes().to_vec(),
            pos: 0,
            chunk,
            delayed: false,
        }
    }
}

impl AsyncRead for TrickleReader {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        if !self.delayed {
            self.delayed = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let to_read = remaining.min(self.chunk).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        self.delayed = false;
        Poll::Ready(Ok(()))
    }
}

pub(crate) struct TrickleStream<S> {
    inner: S,
    delayed: bool,
}

impl<S> TrickleStream<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            delayed: false,
        }
    }
}

impl<S> Stream for TrickleStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if !self.delayed {
            self.delayed = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        self.delayed = false;
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
