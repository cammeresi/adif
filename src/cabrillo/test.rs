use chrono::{NaiveDate, NaiveTime};
use futures::SinkExt;

use crate::{CabrilloSink, Error, Record};

#[tokio::test]
async fn basic() {
    let mut buf = Vec::new();
    let fields = vec!["freq", "mode", "qso_date", "time_on", "call"];
    let mut sink = CabrilloSink::new(&mut buf, fields);

    let mut header = Record::new_header();
    header.insert("contest", "ARRL-SS-CW").unwrap();
    header.insert("callsign", "W1AW").unwrap();
    sink.send(header).await.unwrap();

    let mut qso = Record::new();
    qso.insert("freq", "14000").unwrap();
    qso.insert("mode", "CW").unwrap();
    qso.insert("qso_date", NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
        .unwrap();
    qso.insert("time_on", NaiveTime::from_hms_opt(12, 34, 0).unwrap())
        .unwrap();
    qso.insert("call", "AB9BH").unwrap();
    sink.send(qso).await.unwrap();

    sink.close().await.unwrap();

    let output = String::from_utf8(buf).unwrap();
    let expected = "\
START-OF-LOG: 3.0
CONTEST: ARRL-SS-CW
CALLSIGN: W1AW
QSO: 14000 CW 2020-01-01 1234 AB9BH
END-OF-LOG:
";
    assert_eq!(output, expected);
}

#[tokio::test]
async fn missing_header() {
    let mut buf = Vec::new();
    let fields = vec!["call"];
    let mut sink = CabrilloSink::new(&mut buf, fields);

    let mut qso = Record::new();
    qso.insert("call", "W1AW").unwrap();
    let result = sink.send(qso).await;

    assert_eq!(result.unwrap_err(), Error::MissingHeader);
}

#[tokio::test]
async fn missing_field() {
    let mut buf = Vec::new();
    let fields = vec!["call", "freq"];
    let mut sink = CabrilloSink::new(&mut buf, fields);

    let mut header = Record::new_header();
    header.insert("contest", "TEST").unwrap();
    sink.send(header).await.unwrap();

    let mut qso = Record::new();
    qso.insert("call", "W1AW").unwrap();
    let qso_clone = qso.clone();
    let result = sink.send(qso).await;

    assert_eq!(
        result.unwrap_err(),
        Error::MissingField {
            field: "freq".to_string(),
            record: qso_clone
        }
    );
}

#[tokio::test]
async fn duplicate_header() {
    let mut buf = Vec::new();
    let fields = vec!["call"];
    let mut sink = CabrilloSink::new(&mut buf, fields);

    let mut header1 = Record::new_header();
    header1.insert("contest", "TEST1").unwrap();
    sink.send(header1).await.unwrap();

    let mut header2 = Record::new_header();
    header2.insert("contest", "TEST2").unwrap();
    let result = sink.send(header2).await;

    assert_eq!(result.unwrap_err(), Error::DuplicateHeader);
}

#[tokio::test]
async fn close_error() {
    let mut buf = Vec::new();
    let fields = vec!["call"];
    let mut sink = CabrilloSink::new(&mut buf, fields);

    let result = sink.close().await;
    assert_eq!(result.unwrap_err(), Error::MissingHeader);
}
