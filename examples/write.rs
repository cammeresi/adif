use adif::{Datum, OutputTypes, Record, RecordSink};
use futures::SinkExt;
use rust_decimal::Decimal;
use std::str::FromStr;

fn create_header() -> Result<Record, Box<dyn std::error::Error>> {
    let mut header = Record::new_header();
    header.insert("adifver", "3.1.4")?;
    header.insert("programid", "Example")?;
    Ok(header)
}

fn create_qso(
    call: &str, date: &str, time: &str, freq: &str, mode: &str,
) -> Result<Record, Box<dyn std::error::Error>> {
    let mut record = Record::new();
    record.insert("call", call)?;
    record.insert("qso_date", date)?;
    record.insert("time_on", time)?;
    record.insert("freq", Datum::Number(Decimal::from_str(freq)?))?;
    record.insert("mode", mode)?;
    Ok(record)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, OutputTypes::OnlyNonString);

    sink.send(create_header()?).await?;
    sink.send(create_qso("W1AW", "20240115", "143000", "14.074", "FT8")?)
        .await?;
    sink.send(create_qso("AB9BH", "20240115", "150000", "7.074", "FT8")?)
        .await?;
    sink.close().await?;

    let output = String::from_utf8(buf)?;
    println!("Generated ADIF:");
    println!("{}", output);
    Ok(())
}
