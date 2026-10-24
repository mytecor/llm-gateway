//! Site profiles and workflow/session orchestration. No raw CDP commands.
mod auth;
mod pipeline;
pub mod profile;
mod selectors;
mod session;
mod stream;
use crate::browser::Page;
use anyhow::Result;
use profile::{OutputMode, Profile};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
pub use stream::Delta;
use universal_self_healing_parser::Parser;
struct Observation {
    rule: String,
    busy_rule: String,
    baseline: Vec<String>,
    mode: OutputMode,
}
pub struct Engine {
    pub profile: Profile,
    pub page: Page,
    parser: Parser,
    key: String,
    vars: BTreeMap<String, String>,
    observation: Option<Observation>,
    pub resume_id: Option<String>,
    heal: bool,
    scope_local_actions: bool,
    healed_path: PathBuf,
    pending_system: Option<String>,
    expected_tools: BTreeSet<String>,
    tool_results: BTreeMap<String, String>,
    message_sequence: u64,
    tools_enabled: bool,
}
impl Engine {
    pub async fn new(
        profile: Profile,
        mut page: Page,
        state_dir: &Path,
        heal: bool,
    ) -> Result<Self> {
        let rules_yaml = profile.rules_yaml()?;
        use sha2::{Digest, Sha256};
        // Changes to source rules invalidate the old healing cache.
        let hash = Sha256::digest(rules_yaml.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let healed_path = state_dir.join(format!("rules-{hash}.yaml"));
        let cached_rules = healed_path.exists();
        let source = if cached_rules {
            Some(healed_path.as_path())
        } else {
            None
        };
        let parser = match profile.parser(source, heal).await {
            Ok(parser) => parser,
            Err(error) => {
                let _ = page.close().await;
                return Err(error);
            }
        };
        let key = format!(
            "__web_driver_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        Ok(Self {
            profile,
            page,
            parser,
            key,
            vars: BTreeMap::new(),
            observation: None,
            resume_id: None,
            heal,
            scope_local_actions: !heal && !cached_rules,
            healed_path,
            pending_system: None,
            expected_tools: BTreeSet::new(),
            tool_results: BTreeMap::new(),
            message_sequence: 0,
            tools_enabled: false,
        })
    }
}
