//! Gateway JSON-line message contract and event writer.
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Write};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdp_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cdp_header_env: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub heal: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    #[serde(alias = "describe")]
    Init {
        #[serde(default)]
        id: String,
        #[serde(default)]
        tool_strategy: String,
        #[serde(default)]
        models_url: String,
        #[serde(default)]
        #[allow(dead_code)]
        has_session: bool,
        #[serde(default)]
        #[allow(dead_code)]
        request_timeout_ms: Option<i64>,
        #[serde(default)]
        config: Option<Value>,
    },
    Models {
        #[serde(default)]
        #[allow(dead_code)]
        config: Option<Value>,
    },
    Start {
        #[serde(default)]
        config: Option<Value>,
        #[serde(default)]
        tools: Option<Vec<serde_json::Value>>,
        #[serde(default)]
        resume_id: Option<String>,
        #[serde(default)]
        system: Option<String>,
    },
    Prompt {
        text: String,
    },
    ToolResult {
        id: String,
        content: String,
    },
}
pub fn emit(event: Value) -> Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, &event)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}
