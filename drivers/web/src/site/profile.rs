use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};
use universal_self_healing_parser::{HttpJevClient, Parser};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub version: u32,
    pub site: String,
    pub origin: String,
    pub rules: BTreeMap<String, Value>,
    pub session: Session,
    pub auth: Auth,
    pub pipelines: BTreeMap<String, Vec<Step>>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub url_pattern: String,
    pub on_missing: Missing,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Missing {
    Error,
    Recreate,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    pub authenticated_rule: String,
    #[serde(default)]
    pub challenges: Vec<Challenge>,
    #[serde(default)]
    pub after_login: Vec<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    pub rule: String,
    pub pipeline: String,
    pub prompt: String,
    #[serde(default)]
    pub secret: bool,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Navigate {
        url: String,
    },
    Wait {
        rule: String,
        #[serde(default = "wait_ms")]
        timeout_ms: u64,
    },
    Fill {
        rule: String,
        value: String,
    },
    Click {
        rule: String,
        #[serde(default)]
        pick: Pick,
    },
    Observe {
        rule: String,
        busy_rule: String,
        #[serde(default)]
        mode: OutputMode,
    },
    AwaitResponse {
        #[serde(default = "turn_ms")]
        timeout_ms: u64,
        #[serde(default = "settle_ms")]
        settle_ms: u64,
    },
    CaptureSession {},
    // Each iteration resolves against a fresh snapshot. Only explicit row rules can be deleted.
    Repeat {
        while_rule: String,
        pipeline: String,
        #[serde(default = "max_iterations")]
        max_iterations: usize,
        #[serde(default)]
        wait_for_decrease_ms: u64,
    },
    AssertAbsent {
        rule: String,
    },
}
#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    #[default]
    Auto,
    Text,
    Message,
}
impl OutputMode {
    pub fn resolve(self, tools_enabled: bool) -> Self {
        match self {
            Self::Auto if tools_enabled => Self::Message,
            Self::Auto => Self::Text,
            other => other,
        }
    }
}
#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pick {
    #[default]
    One,
    First,
}
fn wait_ms() -> u64 {
    30_000
}
fn turn_ms() -> u64 {
    900_000
}
fn settle_ms() -> u64 {
    1_000
}
fn max_iterations() -> usize {
    200
}

impl Profile {
    pub fn load(path: &Path) -> Result<Self> {
        let p: Self =
            serde_saphyr::from_str(&std::fs::read_to_string(path)?).context("invalid site YAML")?;
        ensure!(p.version == 1, "unsupported profile version");
        ensure!(
            !p.site.is_empty()
                && p.site
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "invalid site name"
        );
        let origin = url::Url::parse(&p.origin)?;
        ensure!(
            matches!(origin.scheme(), "http" | "https"),
            "invalid site origin"
        );
        let re = regex::Regex::new(&p.session.url_pattern)?;
        ensure!(
            re.captures_len() == 2,
            "session.url_pattern requires exactly one capture group"
        );
        for name in [
            "check_auth",
            "create_thread",
            "resume_thread",
            "prompt",
            "login",
        ] {
            p.pipeline(name)?;
        }
        p.rule(&p.auth.authenticated_rule)?;
        for c in &p.auth.challenges {
            p.rule(&c.rule)?;
            p.pipeline(&c.pipeline)?;
        }
        for name in &p.auth.after_login {
            p.pipeline(name)?;
        }
        for steps in p.pipelines.values() {
            let mut observing = false;
            for step in steps {
                match step {
                    Step::Wait { rule, timeout_ms } => {
                        p.rule(rule)?;
                        ensure!(*timeout_ms > 0, "zero wait timeout");
                    }
                    Step::Fill { rule, .. }
                    | Step::Click { rule, .. }
                    | Step::AssertAbsent { rule } => p.rule(rule)?,
                    Step::Observe {
                        rule, busy_rule, ..
                    } => {
                        p.rule(rule)?;
                        p.rule(busy_rule)?;
                        observing = true;
                    }
                    Step::AwaitResponse {
                        timeout_ms,
                        settle_ms,
                    } => {
                        ensure!(
                            observing,
                            "await_response requires an earlier observe in the same pipeline"
                        );
                        ensure!(
                            *settle_ms > 0 && timeout_ms > settle_ms,
                            "invalid response timeout"
                        );
                        observing = false;
                    }
                    Step::Repeat {
                        while_rule,
                        pipeline,
                        max_iterations,
                        ..
                    } => {
                        p.rule(while_rule)?;
                        p.pipeline(pipeline)?;
                        ensure!(
                            *max_iterations > 0 && *max_iterations <= 1000,
                            "invalid repeat bound"
                        );
                    }
                    Step::Navigate { url } => {
                        ensure!(!url.is_empty(), "empty navigation URL");
                    }
                    _ => {}
                }
            }
        }
        // Reject cycles even though runtime also has a nesting bound.
        for name in p.pipelines.keys() {
            p.check_cycles(name, &mut Vec::new())?;
        }
        Ok(p)
    }
    fn check_cycles<'a>(&'a self, name: &'a str, stack: &mut Vec<&'a str>) -> Result<()> {
        ensure!(!stack.contains(&name), "recursive pipeline: {name}");
        stack.push(name);
        for s in self.pipeline(name)? {
            if let Step::Repeat { pipeline, .. } = s {
                self.check_cycles(pipeline, stack)?;
            }
        }
        stack.pop();
        Ok(())
    }
    fn rule(&self, id: &str) -> Result<()> {
        ensure!(self.rules.contains_key(id), "unknown rule: {id}");
        Ok(())
    }
    pub fn pipeline(&self, name: &str) -> Result<&[Step]> {
        self.pipelines
            .get(name)
            .map(Vec::as_slice)
            .with_context(|| format!("unknown pipeline: {name}"))
    }
    pub fn element_local_rule(&self, id: &str) -> bool {
        fn local(value: &Value) -> bool {
            value.as_object().is_some_and(|object| {
                !object.is_empty()
                    && object.iter().all(|(key, value)| match key.as_str() {
                        "kind" | "regex" => value.is_string(),
                        "all" | "any" => value
                            .as_array()
                            .is_some_and(|a| !a.is_empty() && a.iter().all(local)),
                        "not" => local(value),
                        _ => false,
                    })
            })
        }
        self.rules
            .get(id)
            .and_then(Value::as_object)
            .is_some_and(|object| {
                object.keys().all(|key| {
                    matches!(
                        key.as_str(),
                        "language" | "message" | "rule" | "fixtures" | "metadata"
                    )
                }) && local(&object["rule"])
            })
    }
    pub fn rules_yaml(&self) -> Result<String> {
        let mut docs = Vec::new();
        for (id, rule) in &self.rules {
            let mut rule = rule.clone();
            let obj = rule.as_object_mut().context("rule must be an object")?;
            ensure!(
                !obj.contains_key("id"),
                "rule ID comes from its map key: {id}"
            );
            obj.insert("id".into(), Value::String(id.clone()));
            docs.push(serde_saphyr::to_string(&rule)?);
        }
        Ok(docs.join("\n---\n"))
    }
    pub async fn parser(&self, healed: Option<&Path>, healing: bool) -> Result<Parser> {
        let original = self.rules_yaml()?;
        // Only load persisted rules with exactly the expected IDs. Pipeline config stays authoritative.
        let mut builder = if let Some(path) = healed.filter(|p| p.exists()) {
            Parser::builder().rules_file(path)
        } else {
            Parser::builder().rules_yaml(original.clone())
        };
        if healing {
            let key = std::env::var("JEV_API_KEY").context("--heal requires JEV_API_KEY")?;
            let mut client = HttpJevClient::new(key)?;
            if let Ok(base) = std::env::var("JEV_BASE_URL") {
                client = client.base_url(base);
            }
            builder = builder.jev(client);
        }
        let parser = builder.build().await?;
        let expected = Parser::builder().rules_yaml(original).build().await?;
        if parser.rules()?.ids().collect::<Vec<_>>() != expected.rules()?.ids().collect::<Vec<_>>()
        {
            bail!("saved healing rules do not match this profile; remove the saved rules file");
        }
        Ok(parser)
    }
}
