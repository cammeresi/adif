//#![warn(missing_docs)] // TODO

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::str::FromStr;
use thiserror::Error;

pub mod filter;
pub mod parse;

pub use filter::{NormalizeExt, normalize_times};
pub use parse::{RecordStream, RecordStreamExt, TagDecoder, TagStream};

#[cfg(test)]
mod test;

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
    DateTime(NaiveDateTime),
    String(String),
}

impl Data {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            Self::String(s) => match s.to_uppercase().as_str() {
                "Y" => Some(true),
                "N" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<Decimal> {
        match self {
            Self::Number(n) => Some(*n),
            Self::String(s) => Decimal::from_str(s).ok(),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Self::Date(d) => Some(*d),
            Self::String(s) => NaiveDate::parse_from_str(s, "%Y%m%d").ok(),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Self::Time(t) => Some(*t),
            Self::String(s) => NaiveTime::parse_from_str(s, "%H%M%S").ok(),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<NaiveDateTime> {
        match self {
            Self::DateTime(dt) => Some(*dt),
            Self::String(s) => {
                NaiveDateTime::parse_from_str(s, "%Y%m%d %H%M%S").ok()
            }
            _ => None,
        }
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

    pub fn insert(&mut self, name: String, value: Data) -> Result<(), Error> {
        if self.fields.contains_key(&name) {
            return Err(Error::InvalidFormat(format!(
                "duplicate key: {}",
                name
            )));
        }
        self.fields.insert(name, value);
        Ok(())
    }
}
