use super::{Page, Snapshot};
use crate::dom::range_key;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use universal_self_healing_parser::SourceRange;
impl Snapshot {
    pub async fn resolve(
        &self,
        page: &mut Page,
        key: &str,
        range: SourceRange,
        action: &str,
        value: Option<&str>,
    ) -> Result<Value> {
        let element = self
            .dom
            .elements
            .get(&range_key(range))
            .context("unmapped_element")?;
        ensure!(
            matches!(action, "click" | "fill" | "read"),
            "unknown_action"
        );
        self.validate(page, key).await?;
        if action == "read" {
            return Ok(
                json!({"text":element.text,"visible":element.visible,"enabled":element.enabled}),
            );
        }
        ensure!(element.visible && element.enabled, "element_not_actionable");
        let node = json!({"backendNodeId":element.backend});
        if action == "click" {
            page.command("DOM.scrollIntoViewIfNeeded", node.clone())
                .await?;
            let quads = page.command("DOM.getContentQuads", node.clone()).await?;
            let viewport = page.command("Page.getLayoutMetrics", json!({})).await?;
            let width = viewport["cssLayoutViewport"]["clientWidth"]
                .as_f64()
                .context("missing viewport width")?;
            let height = viewport["cssLayoutViewport"]["clientHeight"]
                .as_f64()
                .context("missing viewport height")?;
            let (x, y) = click_point(&quads, width, height)?;
            let hit = page.command("DOM.getNodeForLocation", json!({"x":x.round() as i64,"y":y.round() as i64,"includeUserAgentShadowDOM":false})).await?;
            // Accept descendants of the selected element, but never an overlay.
            let hit_node = page
                .command(
                    "DOM.describeNode",
                    json!({"backendNodeId":element.backend,"depth":-1,"pierce":false}),
                )
                .await?;
            ensure!(
                contains_node(
                    &hit_node["node"],
                    hit["backendNodeId"]
                        .as_i64()
                        .context("missing hit-test node")?
                ),
                "element_obscured"
            );
            self.validate(page, key).await?;
            // From the first input command onwards the outcome may be ambiguous;
            // no errors below are labelled stale_snapshot or retried by the engine.
            super::input::click(page, x, y).await?;
        } else {
            ensure!(element.editable, "element_not_editable");
            super::input::fill(page, element.backend, value.unwrap_or_default()).await?;
        }
        Ok(json!({"ok":true}))
    }
}
fn contains_node(node: &Value, backend: i64) -> bool {
    node["backendNodeId"].as_i64() == Some(backend)
        || node["children"]
            .as_array()
            .is_some_and(|children| children.iter().any(|n| contains_node(n, backend)))
}
fn click_point(value: &Value, width: f64, height: f64) -> Result<(f64, f64)> {
    for quad in value["quads"].as_array().context("missing content quads")? {
        let values: Vec<f64> = quad
            .as_array()
            .context("invalid content quad")?
            .iter()
            .map(|v| v.as_f64().context("invalid quad coordinate"))
            .collect::<Result<_>>()?;
        if values.len() != 8 {
            continue;
        }
        let x = values.iter().step_by(2).sum::<f64>() / 4.0;
        let y = values.iter().skip(1).step_by(2).sum::<f64>() / 4.0;
        if x >= 0.0 && y >= 0.0 && x < width && y < height {
            return Ok((x, y));
        }
    }
    bail!("element has no clickable point inside viewport")
}
