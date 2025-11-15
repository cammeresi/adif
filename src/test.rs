use futures::StreamExt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

use super::RecordStreamExt;
use super::*;

fn tags(s: &str) -> TagStream<&[u8]> {
    TagDecoder::new_stream(s.as_bytes(), true)
}

async fn next_field<R>(f: &mut TagStream<R>) -> Field
where
    R: AsyncRead + Unpin,
{
    let Tag::Field(field) = f.next().await.unwrap().unwrap() else {
        panic!("expected field");
    };
    field
}

async fn next_eoh<R>(f: &mut TagStream<R>)
where
    R: AsyncRead + Unpin,
{
    let Tag::Eoh = f.next().await.unwrap().unwrap() else {
        panic!("expected eoh");
    };
}

async fn no_tags<R>(f: &mut TagStream<R>)
where
    R: AsyncRead + Unpin,
{
    assert_eq!(f.next().await, None);
}

async fn next_record<S>(f: &mut RecordStream<S>, header: bool) -> Record
where
    S: Stream<Item = Result<Tag, Error>> + Unpin,
{
    let record = f.next().await.unwrap().unwrap();
    assert_eq!(record.is_header(), header);
    record
}

async fn no_records<S>(f: &mut RecordStream<S>)
where
    S: Stream<Item = Result<Tag, Error>> + Unpin,
{
    assert_eq!(f.next().await, None);
}

#[tokio::test]
async fn header() {
    let mut f = tags("Foo Bar Baz <adifver:5>3.1.1 <eoh>");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "adifver");
    assert_eq!(field.value().as_str().unwrap(), "3.1.1");
    next_eoh(&mut f).await;
    no_tags(&mut f).await;
}

#[tokio::test]
async fn typed() {
    let mut f = tags("<foo:3:n>123");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(field.value().as_number().unwrap().to_string(), "123");
    no_tags(&mut f).await;
}

#[tokio::test]
async fn fraction() {
    let mut f = tags("<freq:7:n>14.0705");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "freq");
    let num = field.value().as_number().unwrap();
    assert_eq!(num.to_string(), "14.0705");
    no_tags(&mut f).await;
}

#[tokio::test]
async fn uppercase() {
    let mut f = tags("<FOO:3>Bar");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "foo");
    assert_eq!(field.value(), &Data::String("Bar".to_string()));
    no_tags(&mut f).await;
}

#[tokio::test]
async fn partial_tag_ignore() {
    let s = "<foo:3>ba";
    for i in 0..s.len() {
        let mut f = tags(&s[..=i]);
        no_tags(&mut f).await;
    }
}

#[tokio::test]
async fn partial_tag_error() {
    let s = "<foo:3>ba";
    for i in 0..s.len() {
        let mut f = TagDecoder::new_stream(s[..=i].as_bytes(), false);
        let err = f.next().await.unwrap().unwrap_err();
        match err {
            Error::InvalidFormat(_) => {}
            _ => panic!("expected InvalidFormat error"),
        }
    }
}

struct TrickleReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
}

impl TrickleReader {
    fn new(data: &str, chunk: usize) -> Self {
        Self {
            data: data.as_bytes().to_vec(),
            pos: 0,
            chunk,
        }
    }
}

impl AsyncRead for TrickleReader {
    fn poll_read(
        mut self: Pin<&mut Self>, _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let to_read = remaining.min(self.chunk).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        Poll::Ready(Ok(()))
    }
}

async fn try_trickle(chunk: usize) {
    let reader =
        TrickleReader::new("Foo <bar:3>baz <qux:5:n>12345 <eoh>", chunk);
    let mut f = TagDecoder::new_stream(reader, true);

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "bar");
    assert_eq!(field.value().as_str().unwrap(), "baz");

    let field = next_field(&mut f).await;
    assert_eq!(field.name(), "qux");
    assert_eq!(field.value().as_number().unwrap().to_string(), "12345");

    next_eoh(&mut f).await;
    no_tags(&mut f).await;
}

#[tokio::test]
async fn trickle() {
    try_trickle(1).await;
    try_trickle(2).await;
    try_trickle(3).await;
}

#[tokio::test]
async fn records() {
    let mut f =
        tags("<adifver:5>3.1.4 <eoh><call:4>W1AW<eor><call:5>AB9BH<eor>")
            .records();

    let rec = next_record(&mut f, true).await;
    assert_eq!(rec.get("adifver").unwrap().as_str().unwrap(), "3.1.4");

    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "W1AW");
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "AB9BH");
    no_records(&mut f).await;
}

#[tokio::test]
async fn partial_record_ignore() {
    let mut f = tags("<call:4>W1AW<eor><call:5>AB9BH").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1AW<eor><call:5>AB9B").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1AW<eor>\n   ").records();
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "W1AW");
    no_records(&mut f).await;

    let mut f = tags("<call:4>W1A").records();
    no_records(&mut f).await;
}

#[tokio::test]
async fn partial_record_error() {
    let mut f = RecordStream::new("<call:4>W1A".as_bytes(), false);
    let err = f.next().await.unwrap().unwrap_err();
    match err {
        Error::InvalidFormat(_) => {}
        _ => panic!("expected InvalidFormat error"),
    }
}

#[tokio::test]
async fn record_stream() {
    let mut f = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    let rec = next_record(&mut f, false).await;
    assert_eq!(rec.get("call").unwrap().as_str().unwrap(), "W1AW");
    no_records(&mut f).await;
}

#[tokio::test]
async fn no_length() {
    let mut f = RecordStream::new("<call>W1AW<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    match err {
        Error::InvalidFormat(s) => assert_eq!(s, "call"),
        _ => panic!("expected InvalidFormat error"),
    }
}

#[tokio::test]
async fn too_many() {
    let mut f = RecordStream::new("<call:4:s:xxx>W1AW<eor>".as_bytes(), true);
    let err = f.next().await.unwrap().unwrap_err();
    match err {
        Error::InvalidFormat(s) => assert_eq!(s, "call:4:s:xxx"),
        _ => panic!("expected InvalidFormat error"),
    }
}
