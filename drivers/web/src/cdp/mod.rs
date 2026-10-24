//! Low-level CDP connection and owned target lifecycle. No site or DOM policy.
mod endpoint;
use anyhow::{Context, Result, ensure};
use playwright_cdp::{CdpSession, cdp::connection::CdpConnection};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub struct Cdp {
    connection: Arc<CdpConnection>,
    browser_session: CdpSession,
    page_session: Option<CdpSession>,
    pub target: String,
}
impl Cdp {
    pub async fn connect(endpoint: &str, headers_env: &BTreeMap<String, String>) -> Result<Self> {
        let (ws, websocket_headers) = endpoint::discover(endpoint, headers_env).await?;
        let (connection, _) = tokio::time::timeout(
            Duration::from_secs(15),
            CdpConnection::connect(&ws, Some(websocket_headers)),
        )
        .await
        .context("CDP websocket connection timed out")?
        .map_err(|_| anyhow::anyhow!("CDP websocket connection failed"))?;
        let browser_session = CdpSession::browser(connection.clone());
        browser_session.set_default_timeout_ms(20_000);
        let mut c = Self {
            connection,
            browser_session,
            page_session: None,
            target: String::new(),
        };
        let initialized: Result<()> = async {
            let target = c
                .call(
                    "Target.createTarget",
                    json!({"url":"about:blank", "background":true}),
                    false,
                )
                .await?;
            c.target = target["targetId"]
                .as_str()
                .context("missing target ID")?
                .into();
            let attached = c
                .call(
                    "Target.attachToTarget",
                    json!({"targetId":c.target,"flatten":true}),
                    false,
                )
                .await?;
            let session = attached["sessionId"]
                .as_str()
                .context("missing CDP session ID")?;
            let session = CdpSession::target(c.connection.clone(), session);
            session.set_default_timeout_ms(20_000);
            c.page_session = Some(session);
            c.call("Page.enable", json!({}), true).await?;
            Ok(())
        }
        .await;
        if let Err(error) = initialized {
            let _ = c.close().await;
            return Err(error);
        }
        Ok(c)
    }
    async fn call(&self, method: &str, params: Value, page: bool) -> Result<Value> {
        let session = if page {
            self.page_session
                .as_ref()
                .context("CDP page is not attached")?
        } else {
            &self.browser_session
        };
        // Library errors may contain browser exception text, endpoint credentials,
        // or prompt fragments. Keep those details out of the driver protocol.
        session
            .send(method, params)
            .await
            .map_err(|error| match error {
                playwright_cdp::Error::Timeout(_) => {
                    anyhow::anyhow!("CDP command timed out: {method}")
                }
                playwright_cdp::Error::ChannelClosed => anyhow::anyhow!("CDP disconnected"),
                _ => anyhow::anyhow!("CDP command failed: {method}"),
            })
    }
    /// Send a native page command. This driver never evaluates page JavaScript.
    pub async fn command(&self, method: &str, params: Value) -> Result<Value> {
        ensure!(
            !method.starts_with("Runtime.") && !method.starts_with("Page.addScript"),
            "JavaScript execution is not supported"
        );
        self.call(method, params, true).await
    }
    pub(crate) fn subscribe(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<playwright_cdp::cdp::messages::CdpEvent>> {
        Ok(self
            .page_session
            .as_ref()
            .context("CDP page is not attached")?
            .subscribe())
    }
    pub async fn close(&mut self) -> Result<()> {
        // Never call Browser.close(): this is an attached browser shared with
        // other clients. Close our own target and disconnect even if it is gone.
        let target = if self.target.is_empty() {
            Ok(())
        } else {
            self.call("Target.closeTarget", json!({"targetId":self.target}), false)
                .await
                .map(|_| ())
        };
        let transport = tokio::time::timeout(Duration::from_secs(5), self.connection.close())
            .await
            .context("CDP disconnect timed out")?
            .map_err(|_| anyhow::anyhow!("CDP disconnect failed"));
        target.and(transport)
    }
}
