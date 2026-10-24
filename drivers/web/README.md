# Rust CDP web driver

One binary executes YAML site profiles and embeds `universal-self-healing-parser`
from the sibling checkout `../parser`. The driver and its browser integration
harness are written in Rust; the harness launches an installed Chromium/Chrome
directly. CDP transport and session dispatch use the native Rust
`playwright-cdp` crate (pinned to 0.3.0). The adapter retains HTTP discovery with
proxy headers, endpoint rewriting, origin checks, and application-specific
navigation/snapshot handling. Only the low-level `CdpConnection` / `CdpSession`
API is used. The driver never evaluates or injects JavaScript into the page.
The endpoint must implement `DOMSnapshot.captureSnapshot`, DOM geometry/focus/hit
testing, `Input` mouse/keyboard/text commands (including the `selectAll` editor
command), Page lifecycle events and navigation history. Bridges such as
Foxbridge/Camoufox need the opt-in remote smoke check below; there is no JavaScript
fallback for missing protocol commands.
Rust 1.90+ is required. The browser is externally managed, retains its persistent
profile, and must expose a **browser-level** CDP endpoint.

## Source layout

The dependency direction follows the abstraction levels: `app` runs `site`
workflows; `site` uses `browser` for live operations and `dom` for rule matching;
`browser` combines `cdp` commands with immutable `dom` data. The lower layers do
not import application or site modules. `protocol.rs` is the shared gateway
message contract and event writer.

```text
src/
  main.rs               Tokio entry point
  lib.rs                Shared modules used by the binary and integration tests
  app/
    cli.rs              Command-line arguments
    state.rs            Account directories and process locks
    serve.rs            Incoming JSON-line dispatch
    mod.rs              Application startup and shutdown
  site/
    profile.rs          YAML schema, validation and parser configuration
    selectors.rs        Rule selection, healing persistence and safe retries
    pipeline.rs         Pipeline steps and variable expansion
    session.rs          Conversation creation/resume, prompts and tool results
    stream.rs           Response observation, deltas and completion
    auth.rs             Interactive login and post-login workflows
    mod.rs              Engine state and initialization
  browser/
    page.rs             Page navigation and origin policy
    snapshot.rs         Live capture, scoping and snapshot validation
    actions.rs          Action preparation, hit testing and validation order
    input.rs            Native mouse/keyboard/text input
  dom/
    mod.rs              Immutable snapshot and element data
    wire.rs             Native snapshot response format
    serialize.rs        HTML serialization, byte ranges and captured state
    matching.rs         Parser matching against immutable HTML
  cdp/
    endpoint.rs         HTTP discovery and connection headers
    mod.rs              CDP transport, sessions and owned target lifecycle
  protocol.rs           Gateway messages and outgoing events
```

`browser::Snapshot` owns a `dom::Snapshot`: the browser facade can recapture or
act, while `snapshot.dom.find(...)` matches saved HTML without a browser handle.
Integration tests import the same library modules used by the executable.

## Build and validate

From the gateway repository root:

```sh
make build-web
drivers/web/target/release/web-driver check --profile drivers/web/sites/muse.yaml
```

The previous JavaScript-based adapter was exercised against live Muse through
Camoufox/Foxbridge, including login reuse, cleanup, streaming and session resume.
Those results do not establish compatibility of the native-CDP adapter. Its local
Chromium integration tests cover the workflows below; run the remote smoke check
before using a bridge with the new command requirements. Missing/ambiguous controls
fail rather than selecting arbitrary elements.

Cleanup targets root text-file rows under System files, opens each file editor,
and uses the explicit Delete menu item and destructive confirmation. Directories
and their contents are outside this cleanup scope.

## Remote CLI login

```sh
export WEB_CDP_URL=http://browser.mytecor-homelab.local
drivers/web/target/release/web-driver login \
  --profile drivers/web/sites/muse.yaml --account main
```

No browser window is opened locally. The driver opens one background tab on the
remote browser. Configured challenges prompt on the CLI; password/code inputs use
hidden terminal input. Challenge input is not stored in YAML or driver state.
The remote browser's persistent profile owns cookies and web storage. Keep that
profile/browser alive or persist its data directory on the browser host.

If a reverse proxy protects CDP, provide header values through environment
variables, not command arguments:

```sh
# Set CDP_AUTHORIZATION using your secret manager or shell environment.
web-driver login --profile drivers/web/sites/muse.yaml \
  --cdp-header-env Authorization=CDP_AUTHORIZATION
```

The same headers are used for HTTP discovery and the websocket upgrade. HTTP
redirects are rejected with an actionable error. Direct `ws://` and `wss://`
endpoints are also supported. A bare host must be supplied as a full URL, including
its port if CDP does not use port 80/443. Proxy URL prefixes may require a direct
websocket endpoint. No TLS-verification bypass is provided.

A site that requires unsupported SSO, CAPTCHA or passkey interaction returns
`challenge_unsupported`; the driver does not claim all sites support CLI-only
login. Login is deliberately separate from gateway requests. An expired login
returns `auth_required`; renewing it does not erase saved thread IDs.

After successful `login`, `auth.after_login` pipelines run while holding an
exclusive account lock. Muse keeps system-file cleanup here. `clean` executes
that cleanup explicitly. Neither reconnecting nor restoring a conversation runs
cleanup. Active driver processes hold shared locks, so stop gateway sessions
before logging in or cleaning. Cleanup failure is an error, and login is not
reported ready even though the browser may already have saved authentication.

`--account` namespaces local locks and healed rules; it does **not** create a
browser profile or isolate remote cookies. Use a separate persistent browser/CDP
endpoint per actual account, and keep the same account/state-dir for all processes
using that endpoint. Locks coordinate processes sharing one local state directory;
they do not coordinate independent gateway hosts.

## Gateway and thread continuation

`configs/config.muse.json` runs this binary with `protocol: driver` and
`tool_strategy: prompt`. Paths are relative to the gateway working directory;
build the driver with `make build-web` and start the gateway from the repository
root:

```sh
go run ./cmd/llm-gateway --config configs/config.muse.json serve
```

It uses a new checkpoint file because the legacy driver stored short/incorrect
resume IDs. Existing legacy checkpoints are not migrated automatically.

Keep a stable `session_id` / `X-Session-Affinity` in your client. Gateway history
prefix validation and tool/CWD changes still govern whether a request belongs to
the same session; this driver does not override those rules.

- A process owns a fresh background tab, never another session's tab.
- `create_thread` is used only when there is no resume ID.
- `resume_thread` opens the saved full thread URL, waits for the history to load,
  and verifies its identity. Navigation waits for a new document to finish loading.
  Thread identity comes from native navigation history, including SPA transitions.
- The driver emits `session_updated` once the site assigns an actual thread URL.
  Gateway checkpoints it after the completed turn for restart/idle-eviction resume.
  A failed startup/restore leaves the previous checkpoint intact; it is invalidated
  only after startup succeeds and the next turn can actually submit work.
- `new` is never a valid native thread ID. System instructions are injected only
  on a new thread. Tool results use the current thread.
- `session.on_missing: error` fails closed. `recreate` explicitly opts into the
  gateway's existing `session_not_found` history-replay fallback.
- Process interruption closes only the owned tab and the CDP connection. The
  shared browser and profile remain; the adapter never sends `Browser.close`.
  In-flight turns are not recoverable checkpoints; uncertain submissions are not
  automatically retried.

## Profile format

See [sites/muse.yaml](sites/muse.yaml). `rules` maps stable IDs to native ast-grep
rule documents; the driver supplies each `id` from its key. All pipeline references
and parser rules are checked before connecting. Unsupported fields, unknown rules,
unknown pipelines and recursive pipelines fail validation.

Operations:

| Step | Purpose |
| --- | --- |
| `navigate: {url}` | Navigate within the configured site origin |
| `wait: {rule, timeout_ms}` | Wait for a visible match |
| `fill: {rule, value}` | Fill one enabled, visible form control |
| `click: {rule, pick: one}` | Click one actionable element; `first` must be explicit for lists |
| `observe: {rule, busy_rule, mode}` | Capture the response baseline **before** submission |
| `await_response: {timeout_ms, settle_ms}` | Wait for a new/changed response and generation completion |
| `capture_session: {}` | Publish the current thread URL |
| `repeat: {while_rule, pipeline, max_iterations, wait_for_decrease_ms}` | Bounded loop over fresh snapshots |
| `assert_absent: {rule}` | Fail if any visible match remains |

Variables are `${turn.text}`, `${session.url}` and `${auth.input}`. Interpolation
is a single pass; prompt contents cannot introduce another variable expansion.
`check_auth`, `login`, `create_thread`, `resume_thread` and `prompt` are required
pipelines. The URL regex must have exactly one capture group for the thread ID.

`auto` mode (the default) streams text when no client tools were supplied and
buffers JSON tool envelopes when tools are present. `text` response mode emits append-only deltas and detects rewrites instead of
silently corrupting output. `message` buffers the final response and requires the
JSON tool envelope described in [the gateway protocol](../../docs/session-executors.md).
Use it with `tool_strategy: prompt`; gateway supplies the tool-format instructions.
When the request has no tools, message mode delivers the final plain text instead
of requiring a tool envelope.
No `done` is emitted while tool calls await results, and multiple results are
collected before submitting a follow-up. Message mode is also suitable for sites
that rewrite rendered text, with the tradeoff that output is delivered at completion.
For sites whose rendered Markdown rewrites earlier output, select `message` to
buffer the response; `text` and the text branch of `auto` reject such rewrites.

## Snapshots and healing

`DOMSnapshot.captureSnapshot` captures DOM, layout text and computed styles in one
native browser response. Rust serializes the main light DOM into escaped HTML and
maps UTF-8 byte ranges to backend node IDs, text, visibility and enabled state.
No page attributes, globals, observers or scripts are installed. Response reads
use immutable captured data while the live page continues streaming. Rendered text
comes from layout text with block/line-break separators, rather than `innerText`;
complex rendered Markdown may have different whitespace, so use message mode for
sites that rewrite output. HTML is bounded at 2 MiB; captured text is bounded at
256 KiB per element and 4 MiB total. Rules exceeding that budget fail explicitly.
The parser's snapshot ID and content hash are checked before mapping, and exact
element ranges are required. Void tags use an explicit ` />` ending so the parser
cannot absorb following whitespace into their element ranges.

Before an action, a fresh native snapshot must match the original document,
serialized HTML, backend node identities, captured text/state and input values.
Clicking scrolls the element into view, gets its content quad, checks that the
point hits the element or a descendant, revalidates the snapshot and sends native
mouse events. Filling focuses the backend node, selects its contents with a native
editor command and inserts text (or deletes the selection for an empty value).
Native input produces browser events; change/blur timing follows normal browser
editing rather than a synthetic `change` event on every fill.

For original, unhealed rules containing only element-local kind/regex/boolean
conditions, an action takes a fresh snapshot of the selected element's subtree and
re-matches the rule. Its validation covers that subtree and inherited editable /
disabled state, allowing unrelated animated widgets to keep updating. Context-dependent
and cached/healed rules keep the full-document check. Detached nodes and changed
documents are rejected. Only an explicit pre-action `stale_snapshot` is retried;
focus/input failures with an uncertain outcome are not retried.

Validation and native input are separate protocol commands, not an atomic operation.
A page may change after validation, and changes reverted between captures cannot
be detected. The driver does not claim the former synchronous JavaScript guard's
guarantees. CSS animations may also move a click target between commands.

Version 1 operates on the main frame's light DOM. Script/style/noscript subtrees
are omitted. Iframe documents, shadow-root traversal and network/WebSocket response
extraction are not implemented yet. Response completion uses a configured busy rule
and a quiet interval; it needs a correct profile and is not a guarantee when a site
has no reliable completion indicator.

Healing is opt-in with `--heal` and `JEV_API_KEY` (`JEV_BASE_URL` optional). Native
parser positive/negative fixtures are required for accepted repairs. Repairs are
saved atomically outside the source profile, under
`~/.local/state/llm-gateway/web/<site>/<account>/rules-<source-hash>.yaml`.
Changing source rules invalidates that cache. `WEB_STATE_DIR` / `--state-dir`
overrides its root. Original YAML is never rewritten. Without `--heal`, normal
matching requires no API credentials or network requests to a healing provider.

## Verification

```sh
cargo test --locked --manifest-path drivers/web/Cargo.toml
# Real browser tests require an installed Chromium/Chrome:
make test-web
# For a custom browser executable:
WEB_TEST_CHROMIUM=/path/to/chromium make test-web
```

The browser test is opt-in (ignored by a normal `cargo test`). It discovers Chrome
on standard macOS/Windows paths or Chromium/Chrome on PATH; `WEB_TEST_CHROMIUM`
selects an explicit executable. It creates a temporary browser profile and kills
the owned browser process on completion or failure.

The Rust integration harness starts an isolated headless Chromium and local synthetic
site, then runs the Rust binary over real CDP. It covers CLI login and file cleanup,
Unicode deltas, multiple turns, process restart/resume, strict missing-thread errors,
tool-result continuation, parallel-session isolation, owned-tab cleanup and stale snapshot rejection after
mutation/navigation. A separate native snapshot test runs with page JavaScript
disabled and covers visibility, disabled/readonly controls, contenteditable,
Unicode and empty-value replacement. It does not log in to real services or use Jev.

A separate, opt-in smoke test can attach to an existing HTTP CDP endpoint. It opens
one background tab, loads the explicitly supplied URL, checks a native DOM snapshot and live
URL handling, then verifies its tab is closed and existing tabs remain. It does
not send a model prompt or run login/cleanup pipelines:

```sh
WEB_TEST_REMOTE_CDP=http://127.0.0.1:19222 \
WEB_TEST_REMOTE_URL=https://muse.ai/ \
cargo test --locked --manifest-path drivers/web/Cargo.toml \
  --test remote -- --ignored --nocapture
```

The WebSocket dependency is enabled with native-root rustls support so the library
transport also accepts `wss://` endpoints. Header values still come from environment
variables and transport/browser error details are redacted at the adapter boundary.
