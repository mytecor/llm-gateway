use super::Engine;
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeSet,
    io::{self, Write},
    time::{Duration, Instant},
};
impl Engine {
    pub async fn login(&mut self) -> Result<()> {
        self.run("login").await?;
        let authenticated = self.profile.auth.authenticated_rule.clone();
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut unknown_since = Instant::now();
        let mut handled = BTreeSet::new();
        let mut last_rule = None;
        let mut last_input = Instant::now();
        loop {
            if self.present(&authenticated).await.unwrap_or(false) {
                break;
            }
            ensure!(Instant::now() < deadline, "login timed out");
            let mut recognized = false;
            let mut submitted = false;
            for i in 0..self.profile.auth.challenges.len() {
                let c = &self.profile.auth.challenges[i];
                let (rule, pipeline, prompt, secret) = (
                    c.rule.clone(),
                    c.pipeline.clone(),
                    c.prompt.clone(),
                    c.secret,
                );
                if !self.present(&rule).await.unwrap_or(false) {
                    handled.remove(&rule);
                    continue;
                }
                recognized = true;
                if handled.contains(&rule) {
                    continue;
                }
                let value = login_input(prompt, secret).await?;
                self.vars.insert("auth.input".into(), value);
                let result = self.run(&pipeline).await;
                self.vars.remove("auth.input");
                result?;
                handled.insert(rule.clone());
                last_rule = Some(rule);
                last_input = Instant::now();
                submitted = true;
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
            if recognized {
                unknown_since = Instant::now();
                if !submitted && last_input.elapsed() > Duration::from_secs(5) {
                    if let Some(rule) = &last_rule {
                        handled.remove(rule);
                    }
                    eprintln!("Login step is still active; check the input and try again.");
                }
            }
            ensure!(
                unknown_since.elapsed() < Duration::from_secs(30),
                "challenge_unsupported: no configured login step matched (SSO, CAPTCHA or passkey may need another authentication method)"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        // CLI holds an exclusive account lock; gateway sessions hold shared locks.
        for pipeline in self.profile.auth.after_login.clone() {
            self.run(&pipeline).await?;
        }
        eprintln!("Account authenticated; post-login pipelines completed.");
        Ok(())
    }
}

async fn login_input(prompt: String, secret: bool) -> Result<String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    // A detached OS thread keeps Ctrl-C responsive even while a terminal read blocks.
    std::thread::spawn(move || {
        let result = (|| -> Result<String> {
            if secret {
                return Ok(rpassword::prompt_password(format!("{prompt}: "))?);
            }
            eprint!("{prompt}: ");
            io::stderr().flush()?;
            let mut line = String::new();
            ensure!(io::stdin().read_line(&mut line)? > 0, "login input closed");
            Ok(line.trim_end_matches(['\r', '\n']).into())
        })();
        let _ = tx.send(result);
    });
    rx.await.context("login prompt failed")?
}
