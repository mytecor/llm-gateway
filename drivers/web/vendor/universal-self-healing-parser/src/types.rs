pub use ast_grep_language::SupportLang as Language;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

/// Zero-based UTF-8 byte offsets, with an exclusive end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capture {
    pub name: String,
    pub kind: String,
    pub text: String,
    pub range: SourceRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Match {
    pub rule_id: String,
    pub kind: String,
    pub text: String,
    pub range: SourceRange,
    pub captures: BTreeMap<String, Vec<Capture>>,
}

/// Inputs are borrowed only until `parse` completes; results own all their data.
pub struct ParseRequest<'a> {
    pub language: Language,
    pub content: &'a [u8],
    pub rules: Option<&'a [&'a str]>,
    pub required: &'a [&'a str],
    pub heal: bool,
    pub snapshot_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    NoMatch,
    HealingFailed,
    RevisionConflict,
    InsufficientEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub rule_id: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HealingDiagnostics {
    pub attempted: bool,
    pub updated: Vec<String>,
    pub unresolved: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseResult {
    /// The immutable rule snapshot used to produce all matches in this result.
    pub revision: u64,
    pub snapshot_id: Option<String>,
    pub content_hash: String,
    pub matches: Vec<Match>,
    pub healing: HealingDiagnostics,
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_content_bytes: usize,
    pub max_ast_nodes: usize,
    pub max_matches: usize,
    pub max_candidates_per_rule: usize,
    pub max_candidate_bytes: usize,
    pub max_healed_matches: usize,
    pub max_batch_questions: usize,
    pub max_batch_bytes: usize,
    /// Cooperative deadline, checked between AST/matcher operations.
    pub execution_timeout: Duration,
    pub jev_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_content_bytes: 2 * 1024 * 1024,
            max_ast_nodes: 100_000,
            max_matches: 10_000,
            max_candidates_per_rule: 16,
            max_candidate_bytes: 1024,
            max_healed_matches: 1,
            max_batch_questions: 256,
            max_batch_bytes: 64 * 1024,
            execution_timeout: Duration::from_secs(5),
            jev_timeout: Duration::from_secs(15),
        }
    }
}

/// Trusted fixtures stored in the rule YAML under `fixtures`.
/// Healing requires at least one positive and one negative fixture per rule.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixtures {
    /// Every positive document must contain at least one match.
    #[serde(default)]
    pub positive: Vec<String>,
    /// Negative documents must contain no matches.
    #[serde(default)]
    pub negative: Vec<String>,
}
