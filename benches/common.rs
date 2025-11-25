use adif::{OutputTypes, Record, RecordSink};
use chrono::{Days, NaiveDate, NaiveTime};
use futures::SinkExt;
use rust_decimal::Decimal;
use std::str::FromStr;

const BANDS: &[&str] = &[
    "160M", "80M", "60M", "40M", "30M", "20M", "17M", "15M", "12M", "10M",
    "6M", "2M", "1.25M", "70CM", "33CM", "23CM",
];

const MODES: &[&str] =
    &["FT8", "SSB", "CW", "FT4", "RTTY", "PSK31", "JT65", "FM"];

const STATES: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
];

const POWERS: &[u32] = &[5, 10, 50, 100, 500, 1000, 1500];

fn grid(i: usize) -> String {
    let field = (i / 100) % 324;
    let square = i % 100;
    let f1 = ((field / 18) as u8 + b'A') as char;
    let f2 = ((field % 18) as u8 + b'A') as char;
    let s1 = ((square / 10) as u8 + b'0') as char;
    let s2 = ((square % 10) as u8 + b'0') as char;
    format!("{f1}{f2}{s1}{s2}")
}

fn call(i: usize) -> String {
    let p1 = (((i / 676) % 26) as u8 + b'A') as char;
    let p2 = (((i / 26) % 26) as u8 + b'A') as char;
    let n = ((i % 10) as u8 + b'0') as char;
    let s1 = (((i / 2) % 26) as u8 + b'A') as char;
    let s2 = (((i / 3) % 26) as u8 + b'A') as char;
    let s3 = (((i / 5) % 26) as u8 + b'A') as char;
    format!("{p1}{p2}{n}{s1}{s2}{s3}")
}

fn freq_for_band(band: &str) -> &str {
    match band {
        "160M" => "1.8",
        "80M" => "3.5",
        "60M" => "5.3",
        "40M" => "7.074",
        "30M" => "10.1",
        "20M" => "14.074",
        "17M" => "18.1",
        "15M" => "21.074",
        "12M" => "24.9",
        "10M" => "28.074",
        "6M" => "50.1",
        "2M" => "146.52",
        "1.25M" => "222.1",
        "70CM" => "446.0",
        "33CM" => "902.0",
        "23CM" => "1296.0",
        _ => "14.074",
    }
}

fn header() -> Result<Record, Box<dyn std::error::Error>> {
    let mut h = Record::new_header();
    h.insert("adifver", "3.1.4")?;
    h.insert("programid", "Benchmark")?;
    Ok(h)
}

fn time(h: u32, m: u32, s: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, s).unwrap()
}

fn qso(i: usize) -> Result<Record, Box<dyn std::error::Error>> {
    let mut r = Record::new();

    r.insert("call", call(i))?;

    let base = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let date = base.checked_add_days(Days::new((i % 365) as u64)).unwrap();
    r.insert("qso_date", date)?;
    let date_off = date.checked_add_days(Days::new((i % 3) as u64)).unwrap();
    r.insert("qso_date_off", date_off)?;

    let h = (i % 24) as u32;
    let m = (i % 60) as u32;
    let s = ((i * 7) % 60) as u32;
    r.insert("time_on", time(h, m, s))?;
    let h = (h + 1) % 24;
    let m = (m + ((i % 15) as u32)) % 60;
    r.insert("time_off", time(h, m, s))?;

    let band = BANDS[i % BANDS.len()];
    r.insert("band", band)?;
    let f = freq_for_band(band);
    r.insert("freq", Decimal::from_str(f)?)?;

    r.insert("mode", MODES[i % MODES.len()])?;
    r.insert("gridsquare", grid(i))?;
    r.insert("state", STATES[i % STATES.len()])?;
    let rst = format!("5{}9", (i % 10));
    r.insert("stx", rst.as_str())?;
    r.insert("srx", rst.as_str())?;
    let pwr = POWERS[i % POWERS.len()];
    r.insert("tx_pwr", Decimal::from(pwr))?;

    Ok(r)
}

pub async fn generate(count: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut sink = RecordSink::with_types(&mut buf, OutputTypes::OnlyNonString);

    sink.send(header().unwrap()).await.unwrap();
    for i in 0..count {
        sink.send(qso(i).unwrap()).await.unwrap();
    }
    sink.close().await.unwrap();

    buf
}
