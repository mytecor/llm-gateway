use crate::{Error, Fixtures, Language, Result};
use ast_grep_config::{GlobalRules, RuleConfig, SerializableRuleConfig};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

pub(crate) struct CompiledRule {
    pub raw: Value,
    pub config: RuleConfig<Language>,
    pub fixtures: Fixtures,
}
impl CompiledRule {
    pub fn compile(raw: Value) -> Result<Self> {
        let name = raw
            .get("language")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::InvalidRule("language is required".into()))?;
        name.parse::<Language>()
            .map_err(|_| Error::UnsupportedLanguage(name.into()))?;
        // Parser-specific fixtures are preserved in raw YAML, but are not ast-grep fields.
        let mut native = raw.clone();
        let fixtures: Fixtures = match native.as_object_mut().and_then(|r| r.remove("fixtures")) {
            Some(value) => {
                if !value.is_object() {
                    return Err(Error::InvalidRule("fixtures must be an object".into()));
                }
                serde_json::from_value(value)
                    .map_err(|e| Error::InvalidRule(format!("invalid fixtures: {e}")))?
            }
            None => Fixtures::default(),
        };
        let config: SerializableRuleConfig<Language> =
            serde_json::from_value(native).map_err(|e| Error::InvalidRule(e.to_string()))?;
        if config.id.is_empty() {
            return Err(Error::InvalidRule("id is required".into()));
        }
        let config = RuleConfig::try_from(config, &GlobalRules::default())
            .map_err(|e| Error::InvalidRule(e.to_string()))?;
        Ok(Self {
            raw,
            config,
            fixtures,
        })
    }
    pub fn replace(&self, rule: Value) -> Result<Self> {
        let mut raw = self.raw.clone();
        raw["rule"] = rule;
        Self::compile(raw)
    }
}

/// An immutable, compiled snapshot. Cloning an Arc does not recompile rules.
pub struct RuleSet {
    revision: u64,
    pub(crate) rules: BTreeMap<String, CompiledRule>,
}
impl RuleSet {
    pub fn from_yaml(revision: u64, yaml: &str) -> Result<Self> {
        let docs: Vec<Value> =
            serde_saphyr::from_multiple(yaml).map_err(|e| Error::InvalidRule(e.to_string()))?;
        let mut rules = BTreeMap::new();
        for raw in docs.into_iter().filter(|d| !d.is_null()) {
            let rule = CompiledRule::compile(raw)?;
            let id = rule.config.id.clone();
            if rules.insert(id.clone(), rule).is_some() {
                return Err(Error::InvalidRule(format!("duplicate id: {id}")));
            }
        }
        Ok(Self { revision, rules })
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.rules.keys().map(String::as_str)
    }
    /// Preserves native YAML fields and values, but not comments or formatting.
    pub fn export_yaml(&self) -> Result<String> {
        let docs = self.rules.values().map(|r| &r.raw);
        export(docs)
    }
    pub(crate) fn patched_yaml(&self, patches: &BTreeMap<String, Value>) -> Result<String> {
        let docs: Vec<_> = self
            .rules
            .iter()
            .map(|(id, r)| {
                let mut raw = r.raw.clone();
                if let Some(rule) = patches.get(id) {
                    raw["rule"] = rule.clone();
                }
                raw
            })
            .collect();
        export(docs.iter())
    }
}
fn export<'a>(docs: impl Iterator<Item = &'a Value>) -> Result<String> {
    docs.map(|d| serde_saphyr::to_string(d).map_err(|e| Error::InvalidRule(e.to_string())))
        .collect::<Result<Vec<_>>>()
        .map(|v| v.join("\n---\n"))
}

/// Implementations must atomically compare revisions and publish a fully compiled snapshot.
/// A failed update must leave the previous snapshot intact.
pub trait RuleStore: Send + Sync {
    fn snapshot(&self) -> Result<Arc<RuleSet>>;
    fn compare_and_swap(&self, expected: u64, yaml: &str) -> Result<Arc<RuleSet>>;
}

pub struct MemoryRuleStore {
    state: RwLock<Arc<RuleSet>>,
}
impl MemoryRuleStore {
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        Ok(Self {
            state: RwLock::new(Arc::new(RuleSet::from_yaml(0, yaml)?)),
        })
    }
}
impl RuleStore for MemoryRuleStore {
    fn snapshot(&self) -> Result<Arc<RuleSet>> {
        Ok(self.state.read().map_err(|_| Error::StorePoisoned)?.clone())
    }
    fn compare_and_swap(&self, expected: u64, yaml: &str) -> Result<Arc<RuleSet>> {
        let revision = expected
            .checked_add(1)
            .ok_or(Error::Limit("revision overflow"))?;
        let next = Arc::new(RuleSet::from_yaml(revision, yaml)?);
        let mut guard = self.state.write().map_err(|_| Error::StorePoisoned)?;
        if guard.revision != expected {
            return Err(Error::RevisionConflict {
                expected,
                actual: guard.revision,
            });
        }
        *guard = next.clone();
        Ok(next)
    }
}

/// Atomic file replacement in the same directory; no partial YAML is published.
pub(crate) fn save_snapshot(snapshot: &RuleSet, path: &Path) -> Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(snapshot.export_yaml()?.as_bytes())?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| Error::Io(e.error))?;
    Ok(())
}
