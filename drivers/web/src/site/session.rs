use super::{
    Engine,
    profile::{Missing, OutputMode, Step},
};
use crate::protocol::emit;
use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use std::time::{Duration, Instant};
impl Engine {
    pub async fn start(
        &mut self,
        resume: Option<&str>,
        system: Option<&str>,
        tools_enabled: bool,
    ) -> Result<()> {
        self.tools_enabled = tools_enabled;
        if tools_enabled {
            ensure!(
                self.profile
                    .pipeline("prompt")?
                    .iter()
                    .all(|step| !matches!(
                        step,
                        Step::Observe {
                            mode: OutputMode::Text,
                            ..
                        }
                    )),
                "tools require message response mode"
            );
        }
        if let Some(id) = resume.filter(|s| !s.is_empty()) {
            let url = url::Url::parse(id).context("saved resume ID must be a full thread URL")?;
            self.page.check_url(url.as_str())?;
            let pattern = regex::Regex::new(&self.profile.session.url_pattern)?;
            ensure!(
                pattern.captures(id).is_some_and(|c| &c[1] != "new"),
                "invalid saved thread URL"
            );
            self.vars.insert("session.url".into(), id.into());
            if let Err(error) = self.run("resume_thread").await {
                for i in 0..self.profile.auth.challenges.len() {
                    let rule = self.profile.auth.challenges[i].rule.clone();
                    if self.present(&rule).await.unwrap_or(false) {
                        bail!(
                            "auth_required: run web-driver login, then retry restoring this thread"
                        );
                    }
                }
                return Err(error);
            }
            let actual = self.page.url().await?;
            if actual != id {
                match self.profile.session.on_missing {
                    Missing::Error => {
                        bail!("session_unavailable: saved thread could not be restored")
                    }
                    Missing::Recreate => {
                        emit(json!({"type":"session_not_found"}))?;
                        bail!("saved thread missing");
                    }
                }
            }
            self.resume_id = Some(actual);
        } else {
            self.run("check_auth").await?;
            let auth = self.profile.auth.authenticated_rule.clone();
            let deadline = Instant::now() + Duration::from_secs(15);
            while !self.present(&auth).await.unwrap_or(false) {
                ensure!(
                    Instant::now() < deadline,
                    "auth_required: run web-driver login for this account"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            self.run("create_thread").await?;
            self.pending_system = system.filter(|s| !s.trim().is_empty()).map(str::to_string);
        }
        emit(json!({"type":"ready","resume_id":self.resume_id}))
    }
    pub(super) async fn capture(&mut self) -> Result<()> {
        let url = self.page.url().await?;
        self.page.check_url(&url)?;
        let regex = regex::Regex::new(&self.profile.session.url_pattern)?;
        let capture = regex
            .captures(&url)
            .context("site has not assigned a thread URL")?;
        ensure!(&capture[1] != "new", "site has not assigned a thread ID");
        if self.resume_id.as_deref() != Some(&url) {
            self.resume_id = Some(url.clone());
            emit(json!({"type":"session_updated","resume_id":url}))?;
        }
        Ok(())
    }
    pub async fn prompt(&mut self, text: &str) -> Result<()> {
        ensure!(
            self.expected_tools.is_empty(),
            "tool results are still pending"
        );
        let text = if let Some(system) = self.pending_system.take() {
            format!("[System Instructions]:\n{system}\n\n[User]:\n{text}")
        } else {
            text.into()
        };
        self.vars.insert("turn.text".into(), text);
        let result = self.run("prompt").await;
        self.vars.remove("turn.text");
        result?;
        if self.expected_tools.is_empty() {
            emit(json!({"type":"done"}))?;
        }
        Ok(())
    }
    pub async fn tool_result(&mut self, id: &str, content: &str) -> Result<()> {
        ensure!(
            self.expected_tools.contains(id),
            "unknown or duplicate tool result"
        );
        self.expected_tools.remove(id);
        self.tool_results.insert(id.into(), content.into());
        if self.expected_tools.is_empty() {
            let text = format!(
                "Tool Results:\n{}",
                serde_json::to_string(&self.tool_results)?
            );
            self.tool_results.clear();
            self.prompt(&text).await?;
        }
        Ok(())
    }
}
