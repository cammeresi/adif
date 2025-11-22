use std::borrow::Borrow;
use std::fmt::{Display, Formatter, Result};
use std::hash::{Hash, Hasher};

/// A case-insensitive string that preserves the original case.
///
/// This type stores a string in its original case but implements
/// case-insensitive equality and hashing, making it suitable for
/// use as a key in hash maps that should be case-preserving but wherein
/// case-insensitive lookups are desired.
#[derive(Debug, Clone)]
pub struct CiString(String);

impl CiString {
    /// Returns a string slice of the original case string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Converts this CiString into the underlying String.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for CiString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for CiString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl PartialEq for CiString {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for CiString {}

impl Hash for CiString {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        for b in self.0.bytes() {
            b.to_ascii_lowercase().hash(state);
        }
    }
}

impl Borrow<str> for CiString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Borrow<CiStr> for CiString {
    fn borrow(&self) -> &CiStr {
        CiStr::new(&self.0)
    }
}

impl AsRef<str> for CiString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for CiString {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

/// A case-insensitive borrowed string slice.
///
/// This is the borrowed equivalent of [`CiString`], allowing zero-cost
/// case-insensitive lookups without allocation.
#[derive(Debug)]
#[repr(transparent)]
pub struct CiStr(str);

impl CiStr {
    /// Creates a new `CiStr` from a string slice.
    pub fn new(s: &str) -> &Self {
        // SAFETY: CiStr is repr(transparent) over str
        unsafe { &*(s as *const str as *const CiStr) }
    }
}

impl PartialEq for CiStr {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for CiStr {}

impl Hash for CiStr {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        for b in self.0.bytes() {
            b.to_ascii_lowercase().hash(state);
        }
    }
}

impl Borrow<str> for CiStr {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CiStr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for CiStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[test]
    fn preserves_case() {
        let s = CiString::from("HeLLo".to_string());
        assert_eq!(s.as_str(), "HeLLo");
        assert_eq!(s.to_string(), "HeLLo");
    }

    #[test]
    fn equality_case_insensitive() {
        let a = CiString::from("Hello".to_string());
        let b = CiString::from("hello".to_string());
        let c = CiString::from("HELLO");
        let d = CiString::from("World");

        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(b, c);
        assert_ne!(a, d);
    }

    fn hash<T>(t: &T) -> u64
    where
        T: Hash + ?Sized,
    {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn hash_case_insensitive() {
        let a = CiString::from("Hello".to_string());
        let b = CiString::from("hello".to_string());
        let c = CiString::from("HELLO");
        let d = CiString::from("World");

        assert_eq!(hash(&a), hash(&b));
        assert_eq!(hash(&a), hash(&c));
        assert_eq!(hash(&b), hash(&c));
        assert_ne!(hash(&a), hash(&d));
    }

    #[test]
    fn borrow() {
        let s = CiString::from("Hello".to_string());
        let borrowed: &str = s.borrow();
        assert_eq!(borrowed, "Hello");
    }

    #[test]
    fn as_ref() {
        let s = CiString::from("HeLLo");
        let r: &str = s.as_ref();
        assert_eq!(r, "HeLLo");
    }

    #[test]
    fn display() {
        let s = CiString::from("HeLLo".to_string());
        assert_eq!(format!("{}", s), "HeLLo");
    }

    #[test]
    fn cistr_equality_case_insensitive() {
        let a = CiStr::new("Hello");
        let b = CiStr::new("hello");
        let c = CiStr::new("HELLO");
        let d = CiStr::new("World");

        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(b, c);
        assert_ne!(a, d);
    }

    #[test]
    fn cistr_hash_case_insensitive() {
        let a = CiStr::new("Hello");
        let b = CiStr::new("hello");
        let c = CiStr::new("HELLO");
        let d = CiStr::new("World");

        assert_eq!(hash(a), hash(b));
        assert_eq!(hash(a), hash(c));
        assert_eq!(hash(b), hash(c));
        assert_ne!(hash(a), hash(d));
    }

    #[test]
    fn cistr_cistring_hash_match() {
        let s1 = CiString::from("Hello");
        let s2 = CiStr::new("hello");
        assert_eq!(hash(&s1), hash(s2));
    }

    #[test]
    fn cistr_borrow() {
        let s = CiStr::new("Hello");
        assert_eq!(s.as_ref(), "Hello");
        let b: &str = s.borrow();
        assert_eq!(b, "Hello");
    }

    #[test]
    fn cistr_display() {
        let s = CiStr::new("HeLLo");
        assert_eq!(format!("{}", s), "HeLLo");
    }
}
