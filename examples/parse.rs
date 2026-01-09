use difa::{NormalizeExt, RecordStream};
use futures::StreamExt;
use rust_decimal::prelude::*;

const SAMPLE_ADIF: &[u8] = b"\
This is some ADIF data from WhizzBangHamLogger version 12345!!!

<adifver:5>3.1.4
<programid:6>Example
<eoh>

<call:4>W1AW<qso_date:8>20240115<time_on:6>143000<freq:6>14.074<mode:3>FT8<eor>
<call:5>AB9BH<qso_date:8>20240115<time_on:6>150000<freq:5>7.074<mode:3>FT8<eor>
<call:4>W6RQ<qso_date:8>20240116<time_on:6>120000<freq:6>21.074<mode:3>FT4<eor>
";

fn freq_to_band(freq: Decimal) -> Option<&'static str> {
    let mhz = freq.to_u32()?;
    match mhz {
        7 => Some("40M"),
        14 => Some("20M"),
        21 => Some("15M"),
        28 | 29 => Some("10M"),
        _ => panic!(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = RecordStream::new(SAMPLE_ADIF, true);

    // Add a band field if none exists.
    let mut stream = stream.normalize(|record| {
        if record.is_header() {
            return Ok(());
        }
        if record.get("band").is_some() {
            return Ok(());
        }
        if let Some(freq) = record.get("freq").and_then(|f| f.as_number())
            && let Some(band) = freq_to_band(freq)
        {
            record.insert("band", band)?;
        }
        Ok(())
    });

    while let Some(result) = stream.next().await {
        let record = result?;

        if record.is_header() {
            println!("Header:");
            if let Some(version) = record.get("adifver") {
                println!("  ADIF Version: {}", version.as_str());
            }
            println!();
            continue;
        }

        println!("Contact:");
        if let Some(call) = record.get("call") {
            println!("  Call: {}", call.as_str());
        }
        if let Some(date) = record.get("qso_date") {
            println!("  Date: {}", date.as_str());
        }
        if let Some(time) = record.get("time_on") {
            println!("  Time: {}", time.as_str());
        }
        if let Some(freq) = record.get("freq") {
            println!("  Frequency: {} MHz", freq.as_str());
        }
        if let Some(mode) = record.get("mode") {
            println!("  Mode: {}", mode.as_str());
        }
        if let Some(band) = record.get("band") {
            println!("  Band: {}", band.as_str());
        }
        println!();
    }

    Ok(())
}
