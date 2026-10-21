# Playwright Muse Driver for LLM Gateway

A universal driver for connecting the **Meta Muse (`muse.ai` / Hatch)** personal AI assistant to **LLM Gateway** via the external driver protocol (`executor: "external"`, `protocol: "driver"`).

---

## Architecture and Features

1. **Cross-browser Compatibility**:
   - Built on top of Playwright (Chromium, Firefox, WebKit).
   - Cross-platform (macOS, Linux, Windows).

2. **Persistent Local Profile & Authentication**:
   - The local browser profile (`~/.config/llm-gateway/muse-browser-profile`) is **persisted on disk**.
   - No repeated authentication needed — cookies, session tokens, and browser state remain active between gateway requests.

3. **Cloud/VM Muse System Files Cleanup**:
   - In the Muse UI (left menu: **Library → System Files** / `/library/files`), the assistant accumulates generated files, artifacts, and state.
   - The driver cleans up these **internal Muse system files** (automatically after login or via the `pnpm run clean-files` command), preventing context and memory contamination across sessions.

4. **System Prompts & Tools via Library**:
   - Muse provides a **Library** in the left menu for configuring custom roles and system instructions for the assistant.
   - The driver injects system prompts and function schemas (`tool_strategy: "prompt"`) into the context of each new thread.

5. **Two-Tier Generation Interception**:
   - **WebSocket Interception**: captures network packets to `metaaivm.com` for instant token streaming (TTFT).
   - **DOM Observer Fallback**: monitors text changes directly within the chat UI.
   - **Heartbeats**: emits activity events every 5 seconds.

---

## Quick Start

### 1. Build Drivers (via root pnpm workspace)
```bash
cd drivers
pnpm install
pnpm run build
```

Or individually for Muse:
```bash
cd drivers/muse
pnpm run build
```

### 2. Initial Login to Muse Account
```bash
pnpm run login
```
- A browser window will open.
- Log in to your Muse account.
- The driver will detect the successful login, save the session to the local browser profile, navigate to **Library → System Files**, clean up internal Muse files, and close the window.

### 3. Cleaning Up Muse System Files (Optional)
If Muse has accumulated files in its storage during operation and you want to clean them up:
```bash
pnpm run clean-files
```

### 4. Validate Gateway Configuration
From the repository root:
```bash
go run ./cmd/llm-gateway --config configs/config.muse.json check
```

### 5. Start LLM Gateway
```bash
go run ./cmd/llm-gateway --config configs/config.muse.json serve
```

### 6. Send a Request via OpenAI API
```bash
curl -N http://127.0.0.1:9208/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "muse",
    "messages": [
      {"role": "user", "content": "Hello! Tell me briefly about yourself."}
    ],
    "stream": true
  }'
```
