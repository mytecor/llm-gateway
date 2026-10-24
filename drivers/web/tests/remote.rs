//! Opt-in smoke check against an existing CDP bridge. It never sends a chat prompt.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use web_driver::browser::{Page, Snapshot};

async fn targets(endpoint: &str) -> Result<BTreeSet<String>> {
    let tabs: Vec<Value> = reqwest::get(format!("{}/json/list", endpoint.trim_end_matches('/')))
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(tabs
        .iter()
        .filter_map(|tab| tab["id"].as_str().or_else(|| tab["targetId"].as_str()))
        .map(String::from)
        .collect())
}

#[tokio::test]
#[ignore = "requires WEB_TEST_REMOTE_CDP (HTTP discovery) and WEB_TEST_REMOTE_URL"]
async fn attach_navigate_disconnect_preserves_remote_browser() -> Result<()> {
    let endpoint = std::env::var("WEB_TEST_REMOTE_CDP").context("set WEB_TEST_REMOTE_CDP")?;
    let url = std::env::var("WEB_TEST_REMOTE_URL")
        .context("set WEB_TEST_REMOTE_URL to a safe page to load in a new tab")?;
    let before = targets(&endpoint).await?;
    ensure!(
        !before.is_empty(),
        "remote smoke check needs an existing tab to verify preservation"
    );
    let origin = url::Url::parse(&url)?.origin().ascii_serialization();
    let mut page = Page::connect(&endpoint, &BTreeMap::new(), &origin).await?;
    let own = page.transport.target.clone();
    let result: Result<()> = async {
        page.navigate(&url).await?;
        let actual = page.url().await?;
        page.check_url(&actual)?;
        let snapshot = Snapshot::take(&mut page, "remote-smoke").await?;
        ensure!(!snapshot.dom.html.is_empty(), "empty native DOM snapshot");
        Ok(())
    }
    .await;
    let closed = page.close().await;
    result?;
    closed?;
    let after = targets(&endpoint).await?;
    ensure!(!after.contains(&own), "owned test tab leaked");
    ensure!(before.is_subset(&after), "existing remote tabs disappeared");
    println!(
        "PASS remote CDP attach, native DOM snapshot, navigation and owned-tab cleanup; existing tabs preserved"
    );
    Ok(())
}
