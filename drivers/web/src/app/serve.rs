use crate::{protocol::Input, site::Engine};
use anyhow::{Result, ensure};
use tokio::io::{AsyncBufReadExt, BufReader};
pub(super) async fn serve(engine: &mut Engine) -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut started = false;
    loop {
        // Bound each line before allocating more; same 16 MiB limit as the gateway.
        let mut bytes = Vec::new();
        loop {
            let buf = stdin.fill_buf().await?;
            if buf.is_empty() {
                break;
            }
            let count = buf
                .iter()
                .position(|b| *b == b'\n')
                .map_or(buf.len(), |i| i + 1);
            ensure!(
                bytes.len() + count <= 16 * 1024 * 1024,
                "inbound protocol line too large"
            );
            let done = buf[count - 1] == b'\n';
            bytes.extend_from_slice(&buf[..count]);
            stdin.consume(count);
            if done {
                break;
            }
        }
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let input: Input = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid inbound driver message"))?;
        match input {
            Input::Start {
                resume_id,
                system,
                tools,
            } => {
                ensure!(!started, "driver already started");
                engine
                    .start(
                        resume_id.as_deref(),
                        system.as_deref(),
                        tools.is_some_and(|tools| !tools.is_empty()),
                    )
                    .await?;
                started = true;
            }
            Input::Prompt { text } => {
                ensure!(started, "start is required");
                engine.prompt(&text).await?;
            }
            Input::ToolResult { id, content } => {
                ensure!(started, "start is required");
                engine.tool_result(&id, &content).await?;
            }
        }
    }
}
