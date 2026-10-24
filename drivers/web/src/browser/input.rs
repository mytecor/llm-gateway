//! Native input boundary: errors here must never trigger an automatic action retry.
use super::Page;
use anyhow::Result;
use serde_json::json;
pub(super) async fn click(page: &Page, x: f64, y: f64) -> Result<()> {
    page.command(
        "Input.dispatchMouseEvent",
        json!({"type":"mousePressed","x":x,"y":y,"button":"left","buttons":1,"clickCount":1}),
    )
    .await?;
    page.command(
        "Input.dispatchMouseEvent",
        json!({"type":"mouseReleased","x":x,"y":y,"button":"left","buttons":0,"clickCount":1}),
    )
    .await?;
    Ok(())
}
pub(super) async fn fill(page: &Page, backend: i64, value: &str) -> Result<()> {
    page.command("DOM.focus", json!({"backendNodeId":backend}))
        .await?;
    // Native editor commands avoid platform-dependent Ctrl/Command+A.
    page.command(
        "Input.dispatchKeyEvent",
        json!({"type":"keyDown","key":"Unidentified","commands":["selectAll"]}),
    )
    .await?;
    page.command(
        "Input.dispatchKeyEvent",
        json!({"type":"keyUp","key":"Unidentified"}),
    )
    .await?;
    if value.is_empty() {
        page.command("Input.dispatchKeyEvent", json!({"type":"keyDown","key":"Backspace","code":"Backspace","windowsVirtualKeyCode":8})).await?;
        page.command(
            "Input.dispatchKeyEvent",
            json!({"type":"keyUp","key":"Backspace","code":"Backspace","windowsVirtualKeyCode":8}),
        )
        .await?;
    } else {
        page.command("Input.insertText", json!({"text":value}))
            .await?;
    }
    Ok(())
}
