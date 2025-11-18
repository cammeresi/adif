#![warn(missing_docs)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![cfg_attr(not(test), deny(clippy::panic))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::expect_used))]
#![cfg_attr(not(test), deny(clippy::indexing_slicing))]
#![doc = include_str!("../README.md")]

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::str::FromStr;
use thiserror::Error;

pub mod filter;
pub mod parse;
pub mod write;

#[cfg(test)]
mod test;

pub use filter::{FilterExt, NormalizeExt};
pub use parse::{RecordStream, RecordStreamExt, TagDecoder, TagStream};
pub use write::{OutputTypes, RecordSink, TagEncoder, TagSink, TagSinkExt};

/// Errors that can occur during ADIF parsing and processing.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O error occurred while reading ADIF data.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Invalid ADIF format encountered during parsing.
    ///
    /// This includes malformed tags, invalid type specifiers, or duplicate
    /// keys in records.
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

/// Value for a field in an ADIF record.
///
/// ADIF fields can have various types specified in their tags.  If no type
/// is specified, they default to strings.  This enum represents all possible
/// typed values, and provides methods to coerce between types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    /// Boolean value (type indicator `b` in ADIF tags).
    Boolean(bool),
    /// Numeric value (type indicator `n` in ADIF tags).
    Number(Decimal),
    /// Date value (type indicator `d` in ADIF tags), format YYYYMMDD.
    Date(NaiveDate),
    /// Time value (type indicator `t` in ADIF tags), format HHMMSS.
    Time(NaiveTime),
    /// Combined date and time value.
    DateTime(NaiveDateTime),
    /// String value (default when no type indicator is present).
    String(String),
}

impl Datum {
    /// Return a [bool] value or coerce a string thereto.
    ///
    /// Returns [None] if a string value fails to parse.
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

    /// Return a numeric value as a [Decimal] or coerce a string thereto.
    ///
    /// Returns [None] if a string value fails to parse.
    pub fn as_number(&self) -> Option<Decimal> {
        match self {
            Self::Number(n) => Some(*n),
            Self::String(s) => Decimal::from_str(s).ok(),
            _ => None,
        }
    }

    /// Return a date value as a [NaiveDate] or coerce a string thereto.
    ///
    /// Returns [None] if a string value fails to parse.
    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Self::Date(d) => Some(*d),
            Self::String(s) => NaiveDate::parse_from_str(s, "%Y%m%d").ok(),
            _ => None,
        }
    }

    /// Return a time value as a [NaiveTime] or coerce a string thereto.
    ///
    /// Returns [None] if a string value fails to parse.
    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Self::Time(t) => Some(*t),
            Self::String(s) => NaiveTime::parse_from_str(s, "%H%M%S").ok(),
            _ => None,
        }
    }

    /// Return a datetime value as a [NaiveDateTime] or coerce a string thereto.
    ///
    /// Returns [None] if a string value fails to parse.
    pub fn as_datetime(&self) -> Option<NaiveDateTime> {
        match self {
            Self::DateTime(dt) => Some(*dt),
            Self::String(s) => {
                NaiveDateTime::parse_from_str(s, "%Y%m%d %H%M%S").ok()
            }
            _ => None,
        }
    }

    /// Coerce any datum to a string representation.
    ///
    /// String variants return borrowed data.  All other types are returned in
    /// ADIF format (boolean Y/N, date YYYYMMDD, time HHMMSS).
    pub fn as_str(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::String(s) => Some(Cow::Borrowed(s)),
            Self::Boolean(b) => Some(Cow::Borrowed(if *b { "Y" } else { "N" })),
            Self::Number(n) => Some(Cow::Owned(n.to_string())),
            Self::Date(d) => Some(Cow::Owned(d.format("%Y%m%d").to_string())),
            Self::Time(t) => Some(Cow::Owned(t.format("%H%M%S").to_string())),
            Self::DateTime(dt) => {
                Some(Cow::Owned(dt.format("%Y%m%d %H%M%S").to_string()))
            }
        }
    }
}

impl<T> From<T> for Datum
where
    T: Borrow<str>,
{
    fn from(value: T) -> Self {
        Datum::String(value.borrow().to_string())
    }
}

/// A single tag in an ADIF stream and its associated value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    value: Datum,
}

impl Field {
    /// Return name of the tag.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the value of the tag.
    pub fn value(&self) -> &Datum {
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

/// A single contact record, composed of multiple data fields
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Record {
    header: bool,
    fields: HashMap<String, Datum>,
}

impl Record {
    /// Create a new record.
    ///
    /// ```
    /// use adif::Record;
    /// let mut record = Record::new();
    /// record.insert("call".to_string(), "W1AW".into()).unwrap();
    /// record.insert("freq".to_string(), "14.074".into()).unwrap();
    /// assert_eq!(record.get("call").unwrap().as_str().unwrap(), "W1AW");
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new header record.
    ///
    /// ```
    /// use adif::Record;
    /// let mut header = Record::new_header();
    /// header.insert("adifver".to_string(), "3.1.4".into()).unwrap();
    /// assert!(header.is_header());
    /// assert_eq!(header.get("adifver").unwrap().as_str().unwrap(), "3.1.4");
    /// ```
    pub fn new_header() -> Self {
        Self {
            header: true,
            ..Default::default()
        }
    }

    /// True if this record represents an ADIF header.
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::RecordStream;
    /// use futures::StreamExt;
    /// let mut s = RecordStream::new(
    ///     "<adifver:5>3.1.4<eoh><call:4>W1AW<eor>".as_bytes(),
    ///     true,
    /// );
    /// let header = s.next().await.unwrap().unwrap();
    /// assert!(header.is_header());
    /// let record = s.next().await.unwrap().unwrap();
    /// assert!(!record.is_header());
    /// # });
    /// ```
    pub fn is_header(&self) -> bool {
        self.header
    }

    /// Return the value of the requested field.
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::RecordStream;
    /// use futures::StreamExt;
    /// let mut s = RecordStream::new(
    ///     "<call:4>W1AW<freq:6>14.074<eor>".as_bytes(),
    ///     true,
    /// );
    /// let record = s.next().await.unwrap().unwrap();
    /// assert_eq!(record.get("call").unwrap().as_str().unwrap(), "W1AW");
    /// assert_eq!(record.get("freq").unwrap().as_str().unwrap(), "14.074");
    /// assert!(record.get("missing").is_none());
    /// # });
    /// ```
    pub fn get<Q>(&self, name: &Q) -> Option<&Datum>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fields.get(name)
    }

    /// Return an iterator over all fields in this record.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &Datum)> {
        self.fields.iter()
    }

    /// Add a field to the record.
    ///
    /// Overwriting a previous value is not permitted and will return an
    /// error.  Transformations can only add new keys, not delete or replace
    /// them.
    ///
    /// Since colons cannot occur in tag names, a custom transformation may
    /// wish to convert tag "xxx" to "myapp:xxx".
    ///
    /// ```
    /// # tokio_test::block_on(async {
    /// use adif::{Datum, RecordStream};
    /// use futures::StreamExt;
    /// let mut s = RecordStream::new("<call:4>W1AW<eor>".as_bytes(), true);
    /// let mut record = s.next().await.unwrap().unwrap();
    /// record.insert(
    ///     "band".to_string(),
    ///     Datum::String("20M".to_string()),
    /// )
    /// .unwrap();
    /// assert_eq!(record.get("band").unwrap().as_str().unwrap(), "20M");
    /// let err = record.insert(
    ///     "call".to_string(),
    ///     Datum::String("AB9BH".to_string()),
    /// );
    /// assert!(err.is_err());
    /// # });
    /// ```
    pub fn insert(&mut self, name: String, value: Datum) -> Result<(), Error> {
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
