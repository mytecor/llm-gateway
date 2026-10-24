use super::{
    cli::{Options, header},
    state::{account_dir, lock_account},
};
use crate::{
    browser::Page,
    protocol::{DriverConfig, Input, emit},
    site::{Engine, profile::Profile},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs::File, path::PathBuf, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command as TokioCommand,
};

async fn read_line_bounded<R: tokio::io::AsyncBufRead + Unpin>(stdin: &mut R) -> Result<Vec<u8>> {
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
    Ok(bytes)
}

async fn validate_driver_config(
    id: &str,
    tool_strategy: &str,
    models_url: &str,
    config: Option<Value>,
    require_id: bool,
) -> Result<(DriverConfig, String, Value)> {
    let id = id.trim();
    if require_id && id.is_empty() {
        anyhow::bail!("session providers require id");
    }
    ensure!(
        models_url.trim().is_empty(),
        "provider {id:?}: HTTP settings are not supported by web driver"
    );
    let mut cfg: DriverConfig = match config {
        Some(v) if !v.is_null() => serde_json::from_value(v)
            .map_err(|e| anyhow::anyhow!("provider {id:?}: HTTP settings are not supported by web driver ({e})"))?,
        _ => DriverConfig::default(),
    };
    cfg.profile = cfg.profile.trim().to_string();
    ensure!(
        !cfg.profile.is_empty() || !cfg.command.is_empty(),
        "web driver requires profile or command"
    );
    if !cfg.profile.is_empty() {
        let p = PathBuf::from(&cfg.profile);
        let abs = if p.is_absolute() {
            p
        } else {
            std::env::current_dir()?.join(p)
        };
        if abs.exists() {
            let loaded = Profile::load(&abs)?;
            loaded.parser(None, false).await?;
        }
        cfg.profile = abs.to_string_lossy().into_owned();
    }
    if let Some(s) = cfg.state_dir.take() {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            let p = PathBuf::from(trimmed);
            let abs = if p.is_absolute() {
                p
            } else {
                std::env::current_dir()?.join(p)
            };
            cfg.state_dir = Some(abs.to_string_lossy().into_owned());
        }
    }
    let account = cfg.account.as_deref().unwrap_or("main").trim().to_string();
    ensure!(
        !account.is_empty()
            && account
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "invalid account name"
    );
    for h in &cfg.cdp_header_env {
        header(h).map_err(|e| anyhow::anyhow!(e))?;
    }
    let ts = tool_strategy.trim().to_ascii_lowercase();
    let normalized_ts = match ts.as_str() {
        "" | "auto" => "both".to_string(),
        "mcp" | "prompt" | "wire" | "both" => ts,
        _ => anyhow::bail!("provider {id:?}: unknown tool strategy {tool_strategy:?}"),
    };
    let normalized_val = serde_json::to_value(&cfg)?;
    Ok((cfg, normalized_ts, normalized_val))
}

fn options_from_config(cfg: &DriverConfig) -> Result<Options> {
    let cdp_url = cfg
        .cdp_url
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("WEB_CDP_URL").ok().filter(|s| !s.trim().is_empty()))
        .unwrap_or_else(|| "http://browser.mytecor-homelab.local".to_string());
    let account = cfg
        .account
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "main".to_string());
    let state_dir = cfg
        .state_dir
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("WEB_STATE_DIR").ok().filter(|s| !s.trim().is_empty()))
        .map(PathBuf::from);
    let mut cdp_header_env = Vec::with_capacity(cfg.cdp_header_env.len());
    for h in &cfg.cdp_header_env {
        cdp_header_env.push(header(h).map_err(|e| anyhow::anyhow!(e))?);
    }
    Ok(Options {
        profile: PathBuf::from(&cfg.profile),
        cdp_url,
        account,
        state_dir,
        cdp_header_env,
        heal: cfg.heal,
    })
}

async fn delegate_command<R: tokio::io::AsyncBufRead + Unpin>(
    cmd: &[String],
    first_line: &[u8],
    stdin: &mut R,
) -> Result<()> {
    ensure!(!cmd.is_empty() && !cmd[0].trim().is_empty(), "empty delegate command");
    let mut child = TokioCommand::new(&cmd[0])
        .args(&cmd[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut child_stdin = child.stdin.take().unwrap();
    let mut child_stdout = BufReader::new(child.stdout.take().unwrap());

    child_stdin.write_all(first_line).await?;
    child_stdin.flush().await?;

    let forward_in = async move {
        loop {
            let line = read_line_bounded(stdin).await?;
            if line.is_empty() {
                break;
            }
            if child_stdin.write_all(&line).await.is_err() {
                break;
            }
            let _ = child_stdin.flush().await;
        }
        Ok::<(), anyhow::Error>(())
    };
    let forward_out = async move {
        let mut line = String::new();
        while child_stdout.read_line(&mut line).await? > 0 {
            {
                use std::io::Write;
                let stdout = std::io::stdout();
                let mut out = stdout.lock();
                out.write_all(line.as_bytes())?;
                out.flush()?;
            }
            line.clear();
        }
        Ok::<(), anyhow::Error>(())
    };
    let _ = tokio::join!(forward_in, forward_out);
    let _ = child.kill().await;
    let _ = child.wait().await;
    Ok(())
}

pub(super) async fn serve_stdio() -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut state: Option<(Engine, File)> = None;
    let result = async {
        loop {
            let bytes = read_line_bounded(&mut stdin).await?;
            if bytes.is_empty() {
                return Ok(());
            }
            if bytes.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let input: Input = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid inbound driver message"))?;
            match input {
                Input::Init {
                    id,
                    tool_strategy,
                    models_url,
                    config,
                    ..
                } => {
                    let (_, normalized_ts, normalized_cfg) =
                        validate_driver_config(&id, &tool_strategy, &models_url, config, true)
                            .await?;
                    emit(json!({
                        "type": "ready",
                        "tool_strategy": normalized_ts,
                        "capabilities": {
                            "chat": true,
                            "streaming": true,
                            "tools": true,
                            "stateful": true,
                            "resume": true
                        },
                        "config": normalized_cfg
                    }))?;
                    return Ok(());
                }
                Input::Models { .. } => {
                    emit(json!({"type": "models", "models": []}))?;
                    return Ok(());
                }
                Input::Start {
                    config,
                    resume_id,
                    system,
                    tools,
                } => {
                    ensure!(state.is_none(), "driver already started");
                    let (cfg, _, _) =
                        validate_driver_config("web", "", "", config, false).await?;
                    if !cfg.command.is_empty() && cfg.profile.is_empty() {
                        return delegate_command(&cfg.command, &bytes, &mut stdin).await;
                    }
                    let options = options_from_config(&cfg)?;
                    let profile = Profile::load(&options.profile)?;
                    let dir = account_dir(&options, &profile.site)?;
                    let lock = lock_account(&dir, false)?;
                    profile.parser(None, false).await?;
                    let headers: BTreeMap<_, _> = options.cdp_header_env.iter().cloned().collect();
                    let page = Page::connect(&options.cdp_url, &headers, &profile.origin).await?;
                    let mut engine = Engine::new(profile, page, &dir, options.heal).await?;
                    engine
                        .start(
                            resume_id.as_deref(),
                            system.as_deref(),
                            tools.is_some_and(|tools| !tools.is_empty()),
                        )
                        .await?;
                    state = Some((engine, lock));
                }
                Input::Prompt { text } => {
                    let (engine, _) = state
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("start is required"))?;
                    engine.prompt(&text).await?;
                }
                Input::ToolResult { id, content } => {
                    let (engine, _) = state
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("start is required"))?;
                    engine.tool_result(&id, &content).await?;
                }
            }
        }
    }
    .await;
    if let Some((mut engine, _lock)) = state {
        let closed = engine.page.close().await;
        return result.and(closed);
    }
    result
}

pub(super) async fn serve(engine: &mut Engine) -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut started = false;
    loop {
        let bytes = read_line_bounded(&mut stdin).await?;
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let input: Input = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid inbound driver message"))?;
        match input {
            Input::Init {
                id,
                tool_strategy,
                models_url,
                config,
                ..
            } => {
                let (_, normalized_ts, normalized_cfg) =
                    validate_driver_config(&id, &tool_strategy, &models_url, config, true).await?;
                emit(json!({
                    "type": "ready",
                    "tool_strategy": normalized_ts,
                    "capabilities": {
                        "chat": true,
                        "streaming": true,
                        "tools": true,
                        "stateful": true,
                        "resume": true
                    },
                    "config": normalized_cfg
                }))?;
                return Ok(());
            }
            Input::Models { .. } => {
                emit(json!({"type": "models", "models": []}))?;
                return Ok(());
            }
            Input::Start {
                resume_id,
                system,
                tools,
                ..
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
