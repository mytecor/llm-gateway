use crate::{
    core::{self, Budget},
    flights::{self, Claim, Flights},
    healing,
    store::save_snapshot,
    *,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct Parser {
    store: Arc<dyn RuleStore>,
    flights: Arc<Flights>,
    jev: Option<Arc<dyn JevClient>>,
    limits: Limits,
}
impl Parser {
    pub fn builder() -> ParserBuilder {
        ParserBuilder::default()
    }
    pub fn rules(&self) -> Result<Arc<RuleSet>> {
        self.store.snapshot()
    }
    pub fn export_yaml(&self) -> Result<String> {
        self.rules()?.export_yaml()
    }
    pub async fn save_rules(&self, path: impl AsRef<Path>) -> Result<()> {
        let snapshot = self.rules()?;
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || save_snapshot(&snapshot, &path))
            .await
            .map_err(|_| Error::Io(std::io::Error::other("save task failed")))?
    }

    /// Parses the input once; all normal, candidate and final matching reuse its AST.
    /// A missing required match is returned in diagnostics, preserving other matches.
    /// Syntax errors reject the operation before healing. No network call occurs unless
    /// a required rule is missing, healing is enabled and candidates can be generated.
    pub async fn parse(&self, request: ParseRequest<'_>) -> Result<ParseResult> {
        let mut budget = Budget::new(&self.limits);
        let content = std::str::from_utf8(request.content)?;
        let mut snapshot = self.store.snapshot()?;
        let selected: BTreeSet<String> = match request.rules {
            Some(ids) => ids.iter().map(|id| (*id).to_string()).collect(),
            None => snapshot
                .rules
                .iter()
                .filter(|(_, r)| r.config.language == request.language)
                .map(|(id, _)| id.clone())
                .collect(),
        };
        for id in &selected {
            let rule = snapshot
                .rules
                .get(id)
                .ok_or_else(|| Error::InvalidRequest(format!("unknown rule: {id}")))?;
            if rule.config.language != request.language {
                return Err(Error::InvalidRequest(format!(
                    "rule {id} has a different language"
                )));
            }
        }
        let required: BTreeSet<_> = request
            .required
            .iter()
            .map(|id| (*id).to_string())
            .collect();
        for id in &required {
            if !selected.contains(id) {
                return Err(Error::InvalidRequest(format!(
                    "required rule is not selected: {id}"
                )));
            }
        }
        let ast = core::parse(content, request.language, &self.limits, &budget)?;
        let mut waited = BTreeSet::new();
        let mut leader = None;
        let (mut by_rule, mut total_matches, broken) = loop {
            let mut by_rule = BTreeMap::new();
            let mut total_matches = 0usize;
            for id in &selected {
                let rule = snapshot
                    .rules
                    .get(id)
                    .ok_or_else(|| Error::InvalidRequest(format!("unknown rule: {id}")))?;
                if rule.config.language != request.language {
                    return Err(Error::InvalidRequest(format!(
                        "rule {id} has a different language"
                    )));
                }
                let matches = core::execute(rule, &ast, &self.limits, &budget)?;
                total_matches = total_matches.saturating_add(matches.len());
                if total_matches > self.limits.max_matches {
                    return Err(Error::Limit("total matches"));
                }
                by_rule.insert(id.clone(), matches);
            }
            let broken: Vec<_> = required
                .iter()
                .filter(|id| by_rule[*id].is_empty())
                .cloned()
                .collect();
            let eligible: Vec<_> = broken
                .iter()
                .filter(|id| !waited.contains(*id))
                .cloned()
                .collect();
            if request.heal && self.jev.is_some() && !eligible.is_empty() {
                match self
                    .flights
                    .claim(self.store.as_ref(), snapshot.revision(), &eligible)?
                {
                    Claim::Refresh => {}
                    Claim::Wait(receivers) => {
                        let receivers = receivers
                            .into_iter()
                            .map(|(id, receiver)| {
                                waited.insert(id);
                                receiver
                            })
                            .collect();
                        tokio::time::timeout(self.limits.jev_timeout, flights::wait_all(receivers))
                            .await
                            .map_err(|_| Error::Healing("healing wait timeout".into()))??;
                    }
                    Claim::Lead(guard) => {
                        leader = Some(guard);
                        break (by_rule, total_matches, broken);
                    }
                }
                snapshot = self.store.snapshot()?;
                budget = Budget::new(&self.limits);
                continue;
            }
            break (by_rule, total_matches, broken);
        };
        let outcome = async {
            let mut result = ParseResult {
                revision: snapshot.revision(),
                snapshot_id: request.snapshot_id.map(str::to_string),
                content_hash: Sha256::digest(request.content)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect(),
                matches: Vec::new(),
                healing: HealingDiagnostics {
                    unresolved: broken.clone(),
                    ..Default::default()
                },
            };
            if request.heal && !broken.is_empty() {
                if let Some(jev) = &self.jev {
                    let mut candidates = BTreeMap::new();
                    for id in broken.iter().filter(|id| !waited.contains(*id)) {
                        let cases = &snapshot.rules[id].fixtures;
                        if !healing::has_evidence(cases) {
                            result.healing.diagnostics.push(Diagnostic {
                                code: DiagnosticCode::InsufficientEvidence,
                                rule_id: id.clone(),
                            });
                            continue;
                        }
                        let options =
                            healing::generate(&snapshot.rules[id], &ast, &self.limits, &budget)?;
                        if !options.is_empty() {
                            candidates.insert(id.clone(), options);
                        }
                    }
                    if !candidates.is_empty() {
                        let batch = healing::decision_request(&candidates, &snapshot.rules);
                        if batch.questions.len() > self.limits.max_batch_questions {
                            return Err(Error::Limit("batch questions; batch is never split"));
                        }
                        if serde_json::to_vec(&batch)
                            .map_err(|_| Error::Healing("could not encode batch".into()))?
                            .len()
                            > self.limits.max_batch_bytes
                        {
                            return Err(Error::Limit("batch bytes; batch is never split"));
                        }
                        result.healing.attempted = true;
                        // No store guard is held across this single external call.
                        let response =
                            tokio::time::timeout(self.limits.jev_timeout, jev.decide(batch)).await;
                        // Network wait has its own limit; local validation starts a fresh CPU budget.
                        let budget = Budget::new(&self.limits);
                        let mut patches = BTreeMap::new();
                        let mut pending_matches = BTreeMap::new();
                        let response = response
                            .map_err(|_| Error::Healing("Jev timeout".into()))?
                            .map_err(|error| match error {
                                Error::Healing(_) => error,
                                _ => Error::Healing(error.to_string()),
                            })?;
                        for (id, options) in &candidates {
                            let Some(answer) = response.answers.get(id) else {
                                continue;
                            };
                            let Some(candidate) = options.iter().find(|c| c.id == answer.choice)
                            else {
                                continue;
                            };
                            let Ok(compiled) = snapshot.rules[id].replace(candidate.rule.clone())
                            else {
                                continue;
                            };
                            if healing::validate(
                                &compiled,
                                &ast,
                                &snapshot.rules[id].fixtures,
                                &self.limits,
                                &budget,
                            )? {
                                let matches =
                                    core::execute(&compiled, &ast, &self.limits, &budget)?;
                                total_matches = total_matches.saturating_add(matches.len());
                                if total_matches > self.limits.max_matches {
                                    return Err(Error::Limit("total matches"));
                                }
                                pending_matches.insert(id.clone(), matches);
                                patches.insert(id.clone(), candidate.rule.clone());
                            }
                        }
                        if !patches.is_empty() {
                            let yaml = snapshot.patched_yaml(&patches)?;
                            match self.store.compare_and_swap(snapshot.revision(), &yaml) {
                                Ok(next) => {
                                    result.revision = next.revision();
                                    for id in patches.keys() {
                                        by_rule.insert(
                                            id.clone(),
                                            pending_matches.remove(id).expect("validated matches"),
                                        );
                                        result.healing.updated.push(id.clone());
                                    }
                                }
                                Err(Error::RevisionConflict { .. }) => {
                                    // Keep all original results tied to their original revision.
                                    for id in patches.keys() {
                                        result.healing.diagnostics.push(Diagnostic {
                                            code: DiagnosticCode::RevisionConflict,
                                            rule_id: id.clone(),
                                        });
                                    }
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                } else {
                    for id in &broken {
                        result.healing.diagnostics.push(Diagnostic {
                            code: DiagnosticCode::HealingFailed,
                            rule_id: id.clone(),
                        });
                    }
                }
            }
            result
                .healing
                .unresolved
                .retain(|id| !result.healing.updated.contains(id));
            for id in &result.healing.unresolved {
                result.healing.diagnostics.push(Diagnostic {
                    code: if result.healing.attempted || waited.contains(id) {
                        DiagnosticCode::HealingFailed
                    } else {
                        DiagnosticCode::NoMatch
                    },
                    rule_id: id.clone(),
                });
            }
            result.matches = by_rule.into_values().flatten().collect();
            if result.matches.len() > self.limits.max_matches {
                return Err(Error::Limit("total matches"));
            }
            Ok(result)
        }
        .await;
        if let Some(leader) = leader {
            leader.finish(outcome.as_ref().map(|_| ()).map_err(|error| match error {
                Error::Healing(message) => message.clone(),
                _ => error.to_string(),
            }));
        }
        outcome
    }
}

#[derive(Default)]
pub struct ParserBuilder {
    yaml: Option<String>,
    file: Option<PathBuf>,
    store: Option<Arc<dyn RuleStore>>,
    jev: Option<Arc<dyn JevClient>>,
    limits: Limits,
}
impl ParserBuilder {
    pub fn rules_yaml(mut self, yaml: impl Into<String>) -> Self {
        self.yaml = Some(yaml.into());
        self
    }
    pub fn rules_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.file = Some(path.into());
        self
    }
    pub fn rule_store(mut self, store: Arc<dyn RuleStore>) -> Self {
        self.store = Some(store);
        self
    }
    pub fn jev(mut self, jev: impl JevClient + 'static) -> Self {
        self.jev = Some(Arc::new(jev));
        self
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub async fn build(self) -> Result<Parser> {
        if usize::from(self.yaml.is_some())
            + usize::from(self.file.is_some())
            + usize::from(self.store.is_some())
            > 1
        {
            return Err(Error::InvalidRequest(
                "choose exactly one rule source".into(),
            ));
        }
        if self.limits.max_candidates_per_rule > 254 {
            return Err(Error::InvalidRequest(
                "at most 254 candidates plus none".into(),
            ));
        }
        if self.limits.max_batch_questions > 256 {
            return Err(Error::InvalidRequest("at most 256 batch questions".into()));
        }
        let store = if let Some(store) = self.store {
            store
        } else {
            let yaml = if let Some(file) = self.file {
                tokio::fs::read_to_string(file).await?
            } else {
                self.yaml.unwrap_or_default()
            };
            Arc::new(MemoryRuleStore::from_yaml(&yaml)?)
        };
        let snapshot = store.snapshot()?;
        // Reject malformed trusted fixtures at setup time, before making any Jev call.
        for rule in snapshot.rules.values() {
            for content in rule.fixtures.positive.iter().chain(&rule.fixtures.negative) {
                core::parse(
                    content,
                    rule.config.language,
                    &self.limits,
                    &Budget::new(&self.limits),
                )?;
            }
        }
        Ok(Parser {
            flights: Flights::for_store(&store),
            store,
            jev: self.jev,
            limits: self.limits,
        })
    }
}
