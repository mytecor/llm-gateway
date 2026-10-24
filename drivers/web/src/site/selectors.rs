use super::{Engine, profile::Pick};
use crate::{browser::Snapshot, dom::Element};
use anyhow::{Context, Result, ensure};
use std::time::{Duration, Instant};
impl Engine {
    pub(super) async fn elements(
        &mut self,
        rule: &str,
        required: bool,
    ) -> Result<(Snapshot, Vec<Element>)> {
        let revision = self.parser.rules()?.revision();
        let deadline = Instant::now() + Duration::from_secs(3);
        for attempt in 0..64 {
            let snapshot = Snapshot::take(&mut self.page, &self.key).await?;
            match snapshot
                .dom
                .find(&self.parser, rule, self.heal, required)
                .await
            {
                Ok(elements) => {
                    if self.heal && self.parser.rules()?.revision() != revision {
                        self.parser.save_rules(&self.healed_path).await?;
                    }
                    return Ok((snapshot, elements));
                }
                Err(e)
                    if attempt < 63
                        && Instant::now() < deadline
                        && e.to_string() == "DOM resolution failed: stale_snapshot" =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    pub(super) async fn present(&mut self, rule: &str) -> Result<bool> {
        let (_, els) = self.elements(rule, false).await?;
        Ok(els.iter().any(|e| e.visible))
    }
    pub(super) async fn action(
        &mut self,
        rule: &str,
        action: &str,
        value: Option<&str>,
        pick: Pick,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(3);
        for attempt in 0..64 {
            let result:Result<()> = async {
                let (snapshot,elements)=self.elements(rule,true).await?;
                let candidates:Vec<_>=elements.iter().filter(|e|e.visible&&e.enabled).collect();
                ensure!(!candidates.is_empty() && (candidates.len()==1||matches!(pick,Pick::First)),
                    "rule {rule} must identify one actionable element (or explicitly pick first); found {}",candidates.len());
                let range=candidates[0].range;
                if self.scope_local_actions && self.profile.element_local_rule(rule) {
                    // A pure kind/regex rule depends only on this element's subtree.
                    // Take a fresh scoped snapshot and re-match it; the subsequent
                    // validation watches that entire scope, not unrelated widgets.
                    let scoped=snapshot.scoped(&mut self.page,&self.key,range).await?;
                    let matched=scoped.dom.find(&self.parser,rule,false,true).await?;
                    let target=matched.iter().find(|e|e.range.start==0 && e.range.end==scoped.dom.html.len())
                        .context("element no longer matches the action rule")?;
                    scoped.resolve(&mut self.page,&self.key,target.range,action,value).await?;
                } else {
                    snapshot.resolve(&mut self.page,&self.key,range,action,value).await?;
                }
                Ok(())
            }.await;
            match result {
                Ok(()) => return Ok(()),
                // This rejection proves the action has not occurred. Browser/transport
                // failures with an uncertain outcome never enter this retry path.
                Err(e)
                    if attempt < 63
                        && Instant::now() < deadline
                        && e.to_string() == "DOM resolution failed: stale_snapshot" =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }
}
