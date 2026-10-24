use super::Page;
use crate::dom::{range_key, wire::Capture};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use universal_self_healing_parser::SourceRange;

pub struct Snapshot {
    pub dom: crate::dom::Snapshot,
}
impl Snapshot {
    pub async fn take(page: &mut Page, key: &str) -> Result<Self> {
        Self::capture(page, key, None).await
    }
    pub async fn scoped(&self, page: &mut Page, key: &str, range: SourceRange) -> Result<Self> {
        let element = self
            .dom
            .elements
            .get(&range_key(range))
            .context("unmapped_element")?;
        let snapshot = Self::capture(page, key, Some(element.backend)).await?;
        ensure!(
            snapshot.dom.document == self.dom.document,
            "DOM resolution failed: stale_snapshot"
        );
        Ok(snapshot)
    }
    async fn capture(page: &mut Page, key: &str, root: Option<i64>) -> Result<Self> {
        let url = page.url().await?;
        page.check_url(&url)?;
        let value = page
            .command(
                "DOMSnapshot.captureSnapshot",
                json!({
                    "computedStyles":["display", "visibility", "white-space"],
                    "includeDOMRects":false, "includePaintOrder":false
                }),
            )
            .await
            .context("native DOM snapshots require DOMSnapshot.captureSnapshot support")?;
        let capture: Capture =
            serde_json::from_value(value).context("invalid native DOM snapshot")?;
        page.check_url(capture.url()?)?;
        Ok(Self {
            dom: crate::dom::Snapshot::from_capture(&capture, key, root)?,
        })
    }
    pub(super) async fn validate(&self, page: &mut Page, key: &str) -> Result<()> {
        let current = Self::capture(page, key, Some(self.dom.root)).await?;
        ensure!(
            current.dom.document == self.dom.document
                && current.dom.hash == self.dom.hash
                && current.dom.elements == self.dom.elements,
            "DOM resolution failed: stale_snapshot"
        );
        Ok(())
    }
}
