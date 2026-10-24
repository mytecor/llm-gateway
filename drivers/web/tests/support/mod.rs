use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines},
    net::{TcpListener, TcpStream},
    process::{Child as AsyncChild, ChildStdin, ChildStdout},
    task::JoinHandle,
};

#[derive(Default)]
pub struct SiteState {
    pub threads: BTreeMap<String, Vec<String>>,
    pub files: BTreeSet<String>,
    pub new_thread_visits: usize,
    pub sends: usize,
}
pub struct Site {
    pub origin: String,
    pub state: Arc<Mutex<SiteState>>,
    task: JoinHandle<()>,
}
impl Site {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(SiteState {
            files: ["system-0", "system-1", "user-file"]
                .map(String::from)
                .into(),
            ..Default::default()
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(5), serve(socket, state)).await;
                });
            }
        });
        Ok(Self {
            origin,
            state,
            task,
        })
    }
}
impl Drop for Site {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('"', "&quot;")
}
async fn reply(
    socket: &mut TcpStream,
    status: &str,
    kind: &str,
    body: &str,
    extra: &str,
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    Ok(())
}
async fn serve(mut socket: TcpStream, state: Arc<Mutex<SiteState>>) -> Result<()> {
    let mut data = Vec::new();
    let header_end;
    loop {
        ensure!(data.len() < 16 * 1024, "oversized fixture headers");
        ensure!(
            socket.read_buf(&mut data).await? > 0,
            "incomplete fixture request"
        );
        if let Some(offset) = data.windows(4).position(|v| v == b"\r\n\r\n") {
            header_end = offset + 4;
            break;
        }
    }
    let headers = std::str::from_utf8(&data[..header_end])?.to_string();
    let mut request = headers
        .lines()
        .next()
        .context("missing request")?
        .split_whitespace();
    let method = request.next().context("missing method")?;
    let path = request.next().context("missing path")?;
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map(|(_, v)| v.trim().parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    ensure!(length <= 1024 * 1024, "fixture body too large");
    while data.len() - header_end < length {
        ensure!(
            socket.read_buf(&mut data).await? > 0,
            "truncated fixture body"
        );
    }
    let authenticated = headers.lines().any(|line| {
        line.to_ascii_lowercase().starts_with("cookie:") && line.contains("testauth=1")
    });
    if path == "/snapshot" {
        return reply(
            &mut socket,
            "200 OK",
            "text/html; charset=utf-8",
            include_str!("../fixtures/snapshot.html"),
            "",
        )
        .await;
    }
    if !authenticated {
        return reply(
            &mut socket,
            "200 OK",
            "text/html; charset=utf-8",
            include_str!("../fixtures/login.html"),
            "",
        )
        .await;
    }
    if path == "/send" && method == "POST" {
        let body: Value = serde_json::from_slice(&data[header_end..header_end + length])?;
        let url = body["url"].as_str().context("missing send URL")?;
        let text = body["text"].as_str().context("missing send text")?;
        let response = if text.contains("tools") && !text.starts_with("Tool Results:") {
            json!({"content":"","tool_calls":[{"name":"lookup","arguments":{"id":1}}]}).to_string()
        } else if text.starts_with("Tool Results:") {
            json!({"content":"tool completed 🌏","tool_calls":[]}).to_string()
        } else {
            format!("Response: {text}")
        };
        let id = {
            let mut state = state.lock().unwrap();
            state.sends += 1;
            let id = if url.ends_with("/new") {
                (state.threads.len() + 1).to_string()
            } else {
                url.rsplit('/').next().unwrap().to_string()
            };
            state
                .threads
                .entry(id.clone())
                .or_default()
                .push(response.clone());
            id
        };
        return reply(
            &mut socket,
            "200 OK",
            "application/json",
            &json!({"url":format!("/thread/{id}"),"response":response}).to_string(),
            "",
        )
        .await;
    }
    if method == "POST" && path.starts_with("/delete?") {
        let name = url::form_urlencoded::parse(path.split_once('?').unwrap().1.as_bytes())
            .find(|(k, _)| k == "name")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        let removed = name.starts_with("system-") && state.lock().unwrap().files.remove(&name);
        return reply(
            &mut socket,
            if removed { "200 OK" } else { "403 Forbidden" },
            "text/plain",
            "",
            "",
        )
        .await;
    }
    if path.starts_with("/library/files") {
        let rows=state.lock().unwrap().files.iter().filter(|name|name.starts_with("system-")).map(|name|
            format!("<div data-testid=\"system-file-row\" data-file=\"{name}\"><button aria-haspopup=\"menu\">Menu</button></div>")).collect::<String>();
        let html = include_str!("../fixtures/files.html")
            .replace("__ROWS__", &serde_json::to_string(&rows)?);
        return reply(&mut socket, "200 OK", "text/html; charset=utf-8", &html, "").await;
    }
    let Some(id) = path.strip_prefix("/thread/") else {
        return reply(&mut socket, "404 Not Found", "text/plain", "not found", "").await;
    };
    let messages = {
        let mut state = state.lock().unwrap();
        if id == "new" {
            state.new_thread_visits += 1;
            Some(String::new())
        } else {
            state.threads.get(id).map(|messages| {
                messages
                    .iter()
                    .map(|text| format!("<div data-role=\"assistant\">{}</div>", escape(text)))
                    .collect::<String>()
            })
        }
    };
    let Some(messages) = messages else {
        return reply(
            &mut socket,
            "302 Found",
            "text/plain",
            "",
            "Location: /thread/new\r\n",
        )
        .await;
    };
    // Delay the body after committing navigation, to catch use of the previous/partial document.
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n<html><body>").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    socket
        .write_all(
            include_str!("../fixtures/chat.html")
                .replace("__MESSAGES__", &messages)
                .as_bytes(),
        )
        .await?;
    socket.shutdown().await?;
    Ok(())
}

pub struct Browser {
    process: Child,
    pub endpoint: String,
}
fn chrome_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("WEB_TEST_CHROMIUM") {
        return Ok(path.into());
    }
    let mut candidates = vec![
        PathBuf::from("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        PathBuf::from("/Applications/Chromium.app/Contents/MacOS/Chromium"),
    ];
    for dir in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        for name in [
            "chromium",
            "chromium-browser",
            "google-chrome",
            "google-chrome-stable",
            "chrome.exe",
        ] {
            candidates.push(dir.join(name));
        }
    }
    for var in ["PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"] {
        if let Some(dir) = std::env::var_os(var) {
            candidates.push(PathBuf::from(dir).join("Google/Chrome/Application/chrome.exe"));
        }
    }
    candidates
        .into_iter()
        .find(|p| p.is_file())
        .context("Chromium/Chrome not found; install it or set WEB_TEST_CHROMIUM to its executable")
}
impl Browser {
    pub async fn start(dir: &Path) -> Result<Self> {
        let profile = dir.join("browser");
        std::fs::create_dir_all(&profile)?;
        let log_path = dir.join("chrome.log");
        let log = std::fs::File::create(&log_path)?;
        let process = Command::new(chrome_path()?)
            .args([
                "--headless=new",
                "--remote-debugging-address=127.0.0.1",
                "--remote-debugging-port=0",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--password-store=basic",
            ])
            .arg(format!("--user-data-dir={}", profile.display()))
            .arg("about:blank")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .context("could not launch test Chromium")?;
        let mut browser = Self {
            process,
            endpoint: String::new(),
        };
        for _ in 0..300 {
            if let Ok(port) = std::fs::read_to_string(profile.join("DevToolsActivePort")) {
                let port = port
                    .lines()
                    .next()
                    .context("missing CDP port")?
                    .parse::<u16>()?;
                browser.endpoint = format!("http://127.0.0.1:{port}");
                return Ok(browser);
            }
            if let Some(status) = browser.process.try_wait()? {
                bail!(
                    "Chromium exited with {status}: {}",
                    std::fs::read_to_string(log_path).unwrap_or_default()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bail!("Chromium did not expose CDP within 15 seconds")
    }
}
impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

pub struct Driver {
    process: AsyncChild,
    stdin: Option<ChildStdin>,
    lines: Lines<BufReader<ChildStdout>>,
    stderr: JoinHandle<String>,
}
impl Driver {
    pub fn start(command: &str, profile: &Path, endpoint: &str, dir: &Path) -> Result<Self> {
        let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_web-driver"))
            .arg(command)
            .arg("--profile")
            .arg(profile)
            .arg("--cdp-url")
            .arg(endpoint)
            .arg("--state-dir")
            .arg(dir.join("state"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = process.stdin.take();
        let stdout = process.stdout.take().unwrap();
        let mut stderr = process.stderr.take().unwrap();
        let stderr = tokio::spawn(async move {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text).await;
            text
        });
        Ok(Self {
            process,
            stdin,
            lines: BufReader::new(stdout).lines(),
            stderr,
        })
    }
    pub async fn send(&mut self, message: Value) -> Result<()> {
        let stdin = self.stdin.as_mut().context("driver stdin closed")?;
        stdin.write_all(format!("{message}\n").as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    }
    pub async fn raw_input(&mut self, text: &str) -> Result<()> {
        let mut stdin = self.stdin.take().context("stdin closed")?;
        stdin.write_all(text.as_bytes()).await?;
        stdin.shutdown().await?;
        Ok(())
    }
    pub async fn next(&mut self) -> Result<Value> {
        let line = tokio::time::timeout(Duration::from_secs(15), self.lines.next_line())
            .await
            .context("driver event timed out")??
            .context("driver exited before the next event")?;
        let event: Value = serde_json::from_str(&line)?;
        ensure!(event["type"] != "error", "driver error: {}", event["error"]);
        Ok(event)
    }
    pub async fn wait(&mut self, success: bool) -> Result<()> {
        self.stdin.take();
        let status = match tokio::time::timeout(Duration::from_secs(10), self.process.wait()).await
        {
            Ok(status) => status?,
            Err(_) => {
                self.process.kill().await?;
                let stderr = (&mut self.stderr).await?;
                bail!("driver did not exit: {stderr}");
            }
        };
        let stderr = (&mut self.stderr).await?;
        ensure!(
            status.success() == success,
            "unexpected driver exit {status}: {stderr}"
        );
        Ok(())
    }
    pub async fn turn(&mut self) -> Result<Vec<Value>> {
        let mut events = Vec::new();
        loop {
            let event = self.next().await?;
            let done = event["type"] == "done";
            events.push(event);
            if done {
                return Ok(events);
            }
        }
    }
    pub async fn expect_error(&mut self) -> Result<()> {
        let line = tokio::time::timeout(Duration::from_secs(15), self.lines.next_line())
            .await??
            .context("missing error event")?;
        let event: Value = serde_json::from_str(&line)?;
        ensure!(event["type"] == "error", "expected error, got {event}");
        Ok(())
    }
}
pub fn text(events: &[Value]) -> String {
    events
        .iter()
        .filter(|e| e["type"] == "text")
        .filter_map(|e| e["text"].as_str())
        .collect()
}
pub fn resume(events: &[Value]) -> Result<String> {
    events
        .iter()
        .find(|e| e["type"] == "session_updated")
        .and_then(|e| e["resume_id"].as_str())
        .map(String::from)
        .context("missing native session update")
}
