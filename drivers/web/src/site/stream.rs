use super::{Engine, profile::OutputMode};
use crate::protocol::emit;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
#[derive(Default)]
pub struct Delta {
    pub text: String,
}
impl Delta {
    pub fn update<'a>(&mut self, text: &'a str) -> Result<&'a str> {
        ensure!(
            text.starts_with(&self.text),
            "response rewrote already streamed text; use message mode for this site"
        );
        let delta = &text[self.text.len()..];
        self.text = text.to_string();
        Ok(delta)
    }
}
impl Engine {
    pub(super) async fn stream(&mut self, timeout_ms: u64, settle_ms: u64) -> Result<()> {
        let observation = self
            .observation
            .take()
            .context("observe must precede await_response")?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let mut last_change = Instant::now();
        let mut heartbeat = Instant::now();
        let mut latest = String::new();
        let mut delta = Delta::default();
        let mut seen_response = false;
        loop {
            ensure!(
                Instant::now() < deadline,
                "response timed out; submission may already have occurred"
            );
            let (_, elements) = self.elements(&observation.rule, false).await?;
            let texts: Vec<_> = elements
                .into_iter()
                .filter(|e| e.visible)
                .map(|e| e.text)
                .collect();
            if (texts.len() > observation.baseline.len()
                || (texts.len() == observation.baseline.len()
                    && texts.last() != observation.baseline.last()))
                && let Some(text) = texts.last().filter(|s| !s.is_empty())
            {
                seen_response = true;
                if text != &latest {
                    latest = text.clone();
                    last_change = Instant::now();
                    if matches!(observation.mode, OutputMode::Text) {
                        let chunk = delta.update(&latest)?;
                        if !chunk.is_empty() {
                            emit(json!({"type":"text","text":chunk}))?;
                        }
                    }
                }
            }
            let busy = self.present(&observation.busy_rule).await?;
            if seen_response && !busy && last_change.elapsed() >= Duration::from_millis(settle_ms) {
                break;
            }
            if heartbeat.elapsed() >= Duration::from_secs(5) {
                emit(json!({"type":"heartbeat"}))?;
                heartbeat = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // Update native identity before delivering message/tool calls or done.
        self.capture().await?;
        if matches!(observation.mode, OutputMode::Message) && !self.tools_enabled {
            emit(json!({"type":"text","text":latest}))?;
        }
        if matches!(observation.mode, OutputMode::Message) && self.tools_enabled {
            let message: Value = serde_json::from_str(&latest)
                .context("site response must be a JSON tool envelope in message mode")?;
            ensure!(message["content"].is_string(), "missing message content");
            let calls = message["tool_calls"]
                .as_array()
                .context("missing tool_calls array")?;
            self.message_sequence += 1;
            let id = format!("web-{}", self.message_sequence);
            self.expected_tools = calls
                .iter()
                .enumerate()
                .map(|(i, _)| format!("{id}/{i}"))
                .collect();
            emit(json!({"type":"message","id":id,"text":latest}))?;
        }
        Ok(())
    }
}
