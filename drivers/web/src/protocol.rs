//! Gateway JSON-line message contract and event writer.
use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use std::io::{self, Write};
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    Start {
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
