//! Immutable DOM model and HTML matching. No CDP commands or browser I/O.
mod matching;
mod serialize;
pub(crate) mod wire;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::atomic::AtomicU64};
use universal_self_healing_parser::SourceRange;

static SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const HTML_LIMIT: usize = 2 * 1024 * 1024;
const ELEMENT_TEXT_LIMIT: usize = 256 * 1024;
const TEXT_LIMIT: usize = 4 * 1024 * 1024;

pub struct Snapshot {
    pub id: String,
    pub html: String,
    pub hash: String,
    pub(crate) document: i64,
    pub(crate) root: i64,
    pub(crate) elements: BTreeMap<String, CapturedElement>,
}
#[derive(Debug)]
pub struct Element {
    pub range: SourceRange,
    pub text: String,
    pub visible: bool,
    pub enabled: bool,
}
#[derive(Debug, PartialEq)]
pub(crate) struct CapturedElement {
    pub(crate) backend: i64,
    pub(crate) text: Option<String>,
    pub(crate) visible: bool,
    pub(crate) enabled: bool,
    pub(crate) editable: bool,
    pub(crate) value: Option<String>,
}
pub(crate) fn range_key(range: SourceRange) -> String {
    format!("{}:{}", range.start, range.end)
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
