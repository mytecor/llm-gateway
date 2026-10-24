use super::{
    Engine, Observation,
    profile::{Pick, Step},
};
use anyhow::{Context, Result, ensure};
use std::time::{Duration, Instant};
impl Engine {
    pub fn expand(&self, text: &str) -> Result<String> {
        let mut out = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            rest = &rest[start + 2..];
            let end = rest.find('}').context("unterminated variable")?;
            out.push_str(
                self.vars
                    .get(&rest[..end])
                    .with_context(|| format!("unknown variable: {}", &rest[..end]))?,
            );
            rest = &rest[end + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }
    pub async fn run(&mut self, name: &str) -> Result<()> {
        // Borrow steps independently so recursive execution can mutate the engine.
        let steps = self
            .profile
            .pipelines
            .remove(name)
            .with_context(|| format!("unknown or recursive pipeline: {name}"))?;
        let result = self.run_steps(&steps).await;
        self.profile.pipelines.insert(name.into(), steps);
        result
    }
    async fn run_steps(&mut self, steps: &[Step]) -> Result<()> {
        for step in steps {
            match step {
                Step::Navigate { url } => {
                    let url = self.expand(url)?;
                    self.page.navigate(&url).await?;
                }
                Step::Wait { rule, timeout_ms } => {
                    let deadline = Instant::now() + Duration::from_millis(*timeout_ms);
                    loop {
                        // Read-only retries are safe across navigation and transient DOM updates.
                        if self.present(rule).await.unwrap_or(false) {
                            break;
                        }
                        ensure!(
                            Instant::now() < deadline,
                            "timed out waiting for rule: {rule}"
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
                Step::Fill { rule, value } => {
                    let value = self.expand(value)?;
                    self.action(rule, "fill", Some(&value), Pick::One).await?;
                }
                Step::Click { rule, pick } => self.action(rule, "click", None, *pick).await?,
                Step::Observe {
                    rule,
                    busy_rule,
                    mode,
                } => {
                    let (_, elements) = self.elements(rule, false).await?;
                    self.observation = Some(Observation {
                        rule: rule.clone(),
                        busy_rule: busy_rule.clone(),
                        baseline: elements
                            .into_iter()
                            .filter(|e| e.visible)
                            .map(|e| e.text)
                            .collect(),
                        mode: mode.resolve(self.tools_enabled),
                    });
                }
                Step::AwaitResponse {
                    timeout_ms,
                    settle_ms,
                } => self.stream(*timeout_ms, *settle_ms).await?,
                Step::CaptureSession {} => self.capture().await?,
                Step::AssertAbsent { rule } => {
                    ensure!(!self.present(rule).await?, "rule still present: {rule}")
                }
                Step::Repeat {
                    while_rule,
                    pipeline,
                    max_iterations,
                    wait_for_decrease_ms,
                } => {
                    let mut n = 0;
                    loop {
                        let (_, rows) = self.elements(while_rule, false).await?;
                        let before = rows.iter().filter(|e| e.visible).count();
                        if before == 0 {
                            break;
                        }
                        ensure!(
                            n < *max_iterations,
                            "repeat limit reached for {pipeline}; cleanup incomplete"
                        );
                        Box::pin(self.run(pipeline)).await?;
                        if *wait_for_decrease_ms > 0 {
                            let deadline =
                                Instant::now() + Duration::from_millis(*wait_for_decrease_ms);
                            loop {
                                let (_, rows) = self.elements(while_rule, false).await?;
                                if rows.iter().filter(|e| e.visible).count() < before {
                                    break;
                                }
                                ensure!(
                                    Instant::now() < deadline,
                                    "list did not shrink after {pipeline}; refusing another deletion"
                                );
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                        n += 1;
                    }
                }
            }
        }
        Ok(())
    }
}
