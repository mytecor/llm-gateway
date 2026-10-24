use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

pub(super) async fn discover(
    endpoint: &str,
    headers_env: &BTreeMap<String, String>,
) -> Result<(String, HashMap<String, String>)> {
    let mut headers = reqwest::header::HeaderMap::new();
    let mut websocket_headers = HashMap::new();
    for (name, env) in headers_env {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())?;
        let value =
            std::env::var(env).with_context(|| format!("missing CDP header variable: {env}"))?;
        headers.insert(name.clone(), value.parse()?);
        websocket_headers.insert(name.to_string(), value);
    }
    let endpoint_url = url::Url::parse(endpoint).context("invalid CDP URL")?;
    let ws = if matches!(endpoint_url.scheme(), "http" | "https") {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()?;
        let mut discovery = endpoint_url.clone();
        discovery.set_path(&format!(
            "{}/json/version",
            discovery.path().trim_end_matches('/')
        ));
        let response = client
            .get(discovery)
            .headers(headers.clone())
            .send()
            .await
            .map_err(|_| {
                anyhow::anyhow!("CDP discovery failed; check endpoint and connectivity")
            })?;
        ensure!(
            response.status().is_success(),
            "CDP discovery returned {}; authenticate the CDP proxy or supply --cdp-header-env",
            response.status()
        );
        let info: Value = response
            .json()
            .await
            .context("CDP discovery did not return JSON")?;
        let mut ws = url::Url::parse(
            info["webSocketDebuggerUrl"]
                .as_str()
                .context("missing browser websocket URL")?,
        )?;
        // Chrome commonly advertises localhost behind a reverse proxy. Use the supplied authority.
        ws.set_host(endpoint_url.host_str())?;
        ws.set_port(endpoint_url.port())
            .map_err(|_| anyhow::anyhow!("invalid CDP port"))?;
        ws.set_scheme(if endpoint_url.scheme() == "https" {
            "wss"
        } else {
            "ws"
        })
        .map_err(|_| anyhow::anyhow!("invalid CDP scheme"))?;
        ws.to_string()
    } else {
        ensure!(
            matches!(endpoint_url.scheme(), "ws" | "wss"),
            "unsupported CDP URL scheme"
        );
        endpoint.to_string()
    };
    Ok((ws, websocket_headers))
}
