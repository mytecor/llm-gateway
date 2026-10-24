use crate::cdp::Cdp;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

pub struct Page {
    pub transport: Cdp,
    origin: url::Url,
}
impl Page {
    pub async fn connect(
        endpoint: &str,
        headers_env: &BTreeMap<String, String>,
        origin: &str,
    ) -> Result<Self> {
        let origin = url::Url::parse(origin).context("invalid site origin")?;
        let transport = Cdp::connect(endpoint, headers_env).await?;
        Ok(Self { transport, origin })
    }
    pub async fn command(&self, method: &str, params: Value) -> Result<Value> {
        self.transport.command(method, params).await
    }
    pub async fn close(&mut self) -> Result<()> {
        self.transport.close().await
    }
    pub fn check_url(&self, url: &str) -> Result<()> {
        let u = url::Url::parse(url)?;
        ensure!(
            u.origin() == self.origin.origin(),
            "navigation outside configured site origin"
        );
        Ok(())
    }
    pub async fn navigate(&mut self, url: &str) -> Result<()> {
        self.check_url(url)?;
        let mut events = self.transport.subscribe()?;
        self.command("Page.setLifecycleEventsEnabled", json!({"enabled":true}))
            .await?;
        let result = self.command("Page.navigate", json!({"url":url})).await?;
        ensure!(result.get("errorText").is_none(), "page navigation failed");
        // Same-document navigation has no new loader and needs no load event.
        if let Some(loader) = result["loaderId"].as_str() {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let event = events
                        .recv()
                        .await
                        .context("navigation event stream lost")?;
                    if event.method == "Page.lifecycleEvent"
                        && event.params["loaderId"] == loader
                        && event.params["frameId"] == result["frameId"]
                        && event.params["name"] == "load"
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
            })
            .await
            .context("navigation did not finish loading a new document")??;
        }
        let current = self.url().await?;
        self.check_url(&current)?;
        Ok(())
    }
    pub async fn url(&mut self) -> Result<String> {
        // Navigation history also follows same-document pushState/replaceState.
        let history = self.command("Page.getNavigationHistory", json!({})).await?;
        let index = history["currentIndex"]
            .as_u64()
            .context("missing navigation history index")? as usize;
        Ok(history["entries"][index]["url"]
            .as_str()
            .context("missing live page URL")?
            .into())
    }
}
