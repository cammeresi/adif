//! Writing Cabrillo contest log format

use crate::{Error, Record};
use bytes::{BufMut, BytesMut};
use futures::sink::Sink;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio_util::codec::{Encoder, FramedWrite};

#[cfg(test)]
mod test;

enum Item {
    Record(Record),
    Eof,
}

struct CabrilloEncoder {
    fields: Vec<String>,
    started: bool,
}

impl CabrilloEncoder {
    fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            started: false,
        }
    }

    fn encode_header(
        &mut self, r: Record, dst: &mut BytesMut,
    ) -> Result<(), Error> {
        if self.started {
            return Err(Error::DuplicateHeader);
        }

        self.started = true;
        dst.put_slice(b"START-OF-LOG: 3.0\n");

        for (name, value) in r.fields() {
            let key = name.to_uppercase();
            let val = value.to_cabrillo();

            dst.put_slice(key.as_bytes());
            dst.put_slice(b": ");
            dst.put_slice(val.as_bytes());
            dst.put_slice(b"\n");
        }
        Ok(())
    }

    fn encode_qso(&self, r: Record, dst: &mut BytesMut) -> Result<(), Error> {
        dst.put_slice(b"QSO: ");

        for (i, f) in self.fields.iter().enumerate() {
            let d = r.get(f).ok_or_else(|| Error::MissingField {
                field: f.clone(),
                record: r.clone(),
            })?;

            if i > 0 {
                dst.put_slice(b" ");
            }
            let v = d.to_cabrillo();
            dst.put_slice(v.as_bytes());
        }

        dst.put_slice(b"\n");
        Ok(())
    }
}

impl Encoder<Item> for CabrilloEncoder {
    type Error = Error;

    fn encode(
        &mut self, item: Item, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            Item::Record(r) => {
                if r.is_header() {
                    self.encode_header(r, dst)
                } else if !self.started {
                    Err(Error::MissingHeader)
                } else {
                    self.encode_qso(r, dst)
                }
            }
            Item::Eof => {
                if !self.started {
                    return Err(Error::MissingHeader);
                }
                dst.put_slice(b"END-OF-LOG:\n");
                Ok(())
            }
        }
    }
}

/// Sink for writing Cabrillo format records
pub struct CabrilloSink<W> {
    inner: FramedWrite<W, CabrilloEncoder>,
}

impl<W> CabrilloSink<W>
where
    W: AsyncWrite,
{
    /// Create a new CabrilloSink that writes the specified fields from
    /// each record in Cabrillo format.
    ///
    /// Header field names are output in uppercase.  All values are output
    /// verbatim with no transformation, although the encoder does output
    /// typed data according to the format.
    ///
    /// ```
    /// use adif::{CabrilloSink, Record};
    /// use futures::SinkExt;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut buf = Vec::new();
    /// let fields = vec!["freq", "mode", "time_on", "call"];
    /// let mut sink = CabrilloSink::new(&mut buf, fields);
    ///
    /// let mut header = Record::new_header();
    /// header.insert("contest", "ARRL-SS-CW")?;
    /// sink.send(header).await?;
    ///
    /// let mut qso = Record::new();
    /// qso.insert("freq", "14000")?;
    /// qso.insert("mode", "CW")?;
    /// qso.insert("time_on", "CW")?;
    /// qso.insert("call", "W1AW")?;
    /// sink.send(qso).await?;
    ///
    /// sink.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(w: W, fields: Vec<&str>) -> Self {
        let fields = fields.into_iter().map(|s| s.to_string()).collect();
        Self {
            inner: FramedWrite::new(w, CabrilloEncoder::new(fields)),
        }
    }
}

impl<W> Sink<Record> for CabrilloSink<W>
where
    W: AsyncWrite + Unpin,
{
    type Error = Error;

    fn poll_ready(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map_err(Error::from)
    }

    fn start_send(mut self: Pin<&mut Self>, r: Record) -> Result<(), Error> {
        Pin::new(&mut self.inner).start_send(Item::Record(r))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(Error::from)
    }

    fn poll_close(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.inner).start_send(Item::Eof)?;
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(Error::from)
    }
}
