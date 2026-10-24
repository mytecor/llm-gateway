//! Opt-in real Chromium tests: no Node, package manager, accounts, or paid APIs.
use web_driver::browser::{Page, Snapshot};
mod support;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use support::{Browser, Driver, Site, resume, text};

#[tokio::test]
#[ignore = "requires installed Chromium/Chrome or WEB_TEST_CHROMIUM; run make test-web"]
async fn browser_protocol() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let site = Site::start().await?;
    let browser = Browser::start(temp.path()).await?;
    let profile = temp.path().join("fixture.yaml");
    let source = include_str!("fixture.yaml")
        .replace("https://muse.ai", &site.origin)
        .replace(
            "'^https://muse\\.ai/thread/",
            &format!("'^{}/thread/", regex::escape(&site.origin)),
        )
        .replace("mode: message", "mode: text")
        .replace("settle_ms: 1500", "settle_ms: 300");
    std::fs::write(&profile, &source)?;
    let spawn = |command: &str| Driver::start(command, &profile, &browser.endpoint, temp.path());
    let mut login = spawn("login")?;
    login.raw_input("test@example.test\n").await?;
    login.wait(true).await?;
    assert_eq!(
        site.state.lock().unwrap().files,
        BTreeSet::from(["user-file".to_string()])
    );
    println!("PASS CLI email login and verified system-file cleanup");

    let mut d = spawn("run")?;
    d.send(json!({"type":"start","tools":null,"system":"Be concise."}))
        .await?;
    assert_eq!(d.next().await?["type"], "ready");
    d.send(json!({"type":"prompt","text":"first 🌏"})).await?;
    let events = d.turn().await?;
    let id = resume(&events)?;
    assert!(text(&events).contains("Be concise."));
    d.send(json!({"type":"prompt","text":"second"})).await?;
    assert_eq!(text(&d.turn().await?), "Response: second");
    assert_eq!(site.state.lock().unwrap().threads.len(), 1);
    d.wait(true).await?;
    let visits = site.state.lock().unwrap().new_thread_visits;
    let mut restored = spawn("run")?;
    restored
        .send(json!({"type":"start","resume_id":id,"system":"MUST NOT REPEAT"}))
        .await?;
    assert_eq!(restored.next().await?["resume_id"], id);
    restored
        .send(json!({"type":"prompt","text":"after restart"}))
        .await?;
    assert_eq!(text(&restored.turn().await?), "Response: after restart");
    restored.wait(true).await?;
    {
        let state = site.state.lock().unwrap();
        assert_eq!(state.threads.len(), 1);
        assert_eq!(state.new_thread_visits, visits);
    }
    println!("PASS Unicode deltas, same thread across turns/restart, one-time system prompt");

    let mut missing = spawn("run")?;
    missing
        .send(json!({"type":"start","resume_id":format!("{}/thread/missing",site.origin)}))
        .await?;
    missing.expect_error().await?;
    missing.wait(false).await?;
    assert_eq!(site.state.lock().unwrap().threads.len(), 1);
    println!("PASS missing thread fails without creating a conversation");

    std::fs::write(&profile, source.replace("mode: text", "mode: auto"))?;
    let mut tools = spawn("run")?;
    tools
        .send(json!({"type":"start","tools":[{"type":"function","function":{"name":"lookup"}}]}))
        .await?;
    assert_eq!(tools.next().await?["type"], "ready");
    tools.send(json!({"type":"prompt","text":"tools"})).await?;
    let message = loop {
        let e = tools.next().await?;
        assert_ne!(e["type"], "done");
        if e["type"] == "message" {
            break e;
        }
    };
    tools.send(json!({"type":"tool_result","id":format!("{}/0",message["id"].as_str().context("missing message ID")?),"content":"found"})).await?;
    let events = tools.turn().await?;
    let result: Value = serde_json::from_str(
        events
            .iter()
            .find(|e| e["type"] == "message")
            .context("missing final message")?["text"]
            .as_str()
            .unwrap(),
    )?;
    assert_eq!(result["content"], "tool completed 🌏");
    tools.wait(true).await?;
    assert_eq!(site.state.lock().unwrap().threads.len(), 2);
    println!("PASS tool results continue the same thread; done only after final response");

    std::fs::write(&profile, source.replace("mode: text", "mode: message"))?;
    let mut plain = spawn("run")?;
    plain.send(json!({"type":"start"})).await?;
    assert_eq!(plain.next().await?["type"], "ready");
    plain
        .send(json!({"type":"prompt","text":"buffered plain text"}))
        .await?;
    assert_eq!(text(&plain.turn().await?), "Response: buffered plain text");
    plain.wait(true).await?;
    println!("PASS message mode accepts plain text without tools");

    std::fs::write(&profile, source.replace("mode: text", "mode: auto"))?;
    let mut a = spawn("run")?;
    let mut b = spawn("run")?;
    a.send(json!({"type":"start"})).await?;
    b.send(json!({"type":"start"})).await?;
    assert_eq!(a.next().await?["type"], "ready");
    assert_eq!(b.next().await?["type"], "ready");
    a.send(json!({"type":"prompt","text":"parallel A"})).await?;
    b.send(json!({"type":"prompt","text":"parallel B"})).await?;
    let (ea, eb) = tokio::try_join!(a.turn(), b.turn())?;
    assert_ne!(resume(&ea)?, resume(&eb)?);
    assert_eq!(text(&ea), "Response: parallel A");
    assert_eq!(text(&eb), "Response: parallel B");
    tokio::try_join!(a.wait(true), b.wait(true))?;
    println!("PASS simultaneous sessions use separate tabs and threads");

    let mut streaming = spawn("run")?;
    streaming.send(json!({"type":"start"})).await?;
    assert_eq!(streaming.next().await?["type"], "ready");
    let long = format!("long 🌏 {}", "x".repeat(420));
    streaming.send(json!({"type":"prompt","text":long})).await?;
    let events = streaming.turn().await?;
    ensure!(
        events.iter().filter(|e| e["type"] == "text").count() > 1,
        "response was buffered during DOM mutations"
    );
    assert_eq!(text(&events), format!("Response: {long}"));
    streaming.wait(true).await?;
    println!("PASS incremental Unicode response during continuous DOM mutations");
    let tabs: Vec<Value> = reqwest::get(format!("{}/json/list", browser.endpoint))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        !tabs.iter().any(|tab| tab["url"]
            .as_str()
            .is_some_and(|url| url.starts_with(&site.origin))),
        "driver leaked owned tabs"
    );
    println!(
        "All browser integration checks passed; {} submissions.",
        site.state.lock().unwrap().sends
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires installed Chromium/Chrome or WEB_TEST_CHROMIUM"]
async fn native_snapshots() {
    let temp = tempfile::tempdir().unwrap();
    let site = Site::start().await.unwrap();
    let browser = Browser::start(temp.path()).await.unwrap();
    snapshot_invalidation(&browser.endpoint, &site.origin).await;
}

async fn node(cdp: &Page, selector: &str) -> i64 {
    let doc = cdp.command("DOM.getDocument", json!({})).await.unwrap();
    cdp.command(
        "DOM.querySelector",
        json!({"nodeId":doc["root"]["nodeId"],"selector":selector}),
    )
    .await
    .unwrap()["nodeId"]
        .as_i64()
        .unwrap()
}
async fn set_attr(cdp: &Page, selector: &str, name: &str, value: &str) {
    cdp.command(
        "DOM.setAttributeValue",
        json!({"nodeId":node(cdp,selector).await,"name":name,"value":value}),
    )
    .await
    .unwrap();
}
async fn textarea_value(cdp: &Page) -> String {
    let snapshot = cdp
        .command("DOMSnapshot.captureSnapshot", json!({"computedStyles":[]}))
        .await
        .unwrap();
    let nodes = &snapshot["documents"][0]["nodes"];
    let names = nodes["nodeName"].as_array().unwrap();
    let index = names
        .iter()
        .position(|n| snapshot["strings"][n.as_u64().unwrap() as usize] == "TEXTAREA")
        .unwrap();
    let rare = &nodes["textValue"];
    let value = rare["index"]
        .as_array()
        .unwrap()
        .iter()
        .position(|n| n.as_u64() == Some(index as u64))
        .unwrap();
    let string_index = rare["value"][value].as_i64().unwrap();
    if string_index == -1 {
        return String::new();
    }
    snapshot["strings"][string_index as usize]
        .as_str()
        .unwrap()
        .into()
}
async fn snapshot_invalidation(endpoint: &str, origin: &str) {
    use universal_self_healing_parser::{Language, ParseRequest, Parser};
    let mut cdp = Page::connect(endpoint, &BTreeMap::new(), origin)
        .await
        .unwrap();
    cdp.navigate(&format!("{origin}/snapshot")).await.unwrap();
    cdp.command(
        "Emulation.setScriptExecutionDisabled",
        json!({"value":true}),
    )
    .await
    .unwrap();
    // A static fixture and native DOM commands exercise the full path without
    // Runtime.evaluate, including setup and assertions.
    let key = "snapshot_test";
    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    let rules = [
        "pre", "hidden", "disabled", "aria", "legend", "fieldset", "readonly", "editable", "input",
    ]
    .iter()
    .map(|id| {
        format!(
            "id: {id}\nlanguage: Html\nrule:\n  kind: element\n  regex: '^<[^>]* id=\"{id}\"'\n"
        )
    })
    .collect::<Vec<_>>()
    .join("---\n");
    let controls = Parser::builder().rules_yaml(&rules).build().await.unwrap();
    for (id, visible, enabled) in [
        ("hidden", false, true),
        ("disabled", true, false),
        ("aria", true, false),
        ("fieldset", true, false),
        ("legend", true, true),
        ("readonly", true, true),
    ] {
        let elements = snapshot
            .dom
            .find(&controls, id, false, true)
            .await
            .unwrap_or_else(|e| panic!("{id}: {e}"));
        assert_eq!(elements.len(), 1, "{id}");
        assert_eq!(
            (elements[0].visible, elements[0].enabled),
            (visible, enabled),
            "{id}"
        );
        if !visible || !enabled || id == "readonly" {
            assert!(
                snapshot
                    .resolve(
                        &mut cdp,
                        key,
                        elements[0].range,
                        "fill",
                        Some("must not apply")
                    )
                    .await
                    .is_err(),
                "{id}"
            );
        }
    }
    assert_eq!(
        snapshot
            .dom
            .find(&controls, "pre", false, true)
            .await
            .unwrap()[0]
            .text,
        "first\n  second 🌏"
    );
    let editable = snapshot
        .dom
        .find(&controls, "editable", false, true)
        .await
        .unwrap()[0]
        .range;
    snapshot
        .resolve(&mut cdp, key, editable, "fill", Some("new 🌏 value"))
        .await
        .unwrap();
    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    assert_eq!(
        snapshot
            .dom
            .find(&controls, "editable", false, true)
            .await
            .unwrap()[0]
            .text,
        "new 🌏 value"
    );
    let parser = Parser::builder()
        .rules_yaml("id: input\nlanguage: Html\nrule:\n  kind: element\n  regex: '^<textarea'\n")
        .build()
        .await
        .unwrap();
    let parsed = parser
        .parse(ParseRequest {
            language: Language::Html,
            content: snapshot.dom.html.as_bytes(),
            rules: None,
            required: &[],
            heal: false,
            snapshot_id: Some(&snapshot.dom.id),
        })
        .await
        .unwrap();
    let range = parsed.matches[0].range;
    assert!(parsed.matches[0].text.contains("🌏 Привет &amp; &lt; &gt;"));
    snapshot
        .resolve(&mut cdp, key, range, "fill", Some("🌏 exact value"))
        .await
        .unwrap();
    assert_eq!(textarea_value(&cdp).await, "🌏 exact value");
    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    let captured_text = snapshot
        .dom
        .find(&parser, "input", false, false)
        .await
        .unwrap()[0]
        .text
        .clone();
    set_attr(&cdp, "body", "data-changed", "yes").await;
    assert_eq!(
        snapshot
            .dom
            .find(&parser, "input", false, false)
            .await
            .unwrap()[0]
            .text,
        captured_text
    );
    assert!(
        snapshot
            .resolve(&mut cdp, key, range, "fill", Some("must not apply"))
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_snapshot")
    );
    assert_eq!(textarea_value(&cdp).await, "🌏 exact value");
    let scoped = snapshot.scoped(&mut cdp, key, range).await.unwrap();
    let local = scoped
        .dom
        .find(&parser, "input", false, false)
        .await
        .unwrap();
    assert_eq!(local.len(), 1);
    assert!(local[0].visible && local[0].enabled);
    set_attr(&cdp, "body", "data-unrelated", "another change").await;
    scoped
        .resolve(&mut cdp, key, local[0].range, "fill", Some("scoped value"))
        .await
        .unwrap();
    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    let range = snapshot
        .dom
        .find(&parser, "input", false, true)
        .await
        .unwrap()[0]
        .range;
    let scoped = snapshot.scoped(&mut cdp, key, range).await.unwrap();
    let local = scoped
        .dom
        .find(&parser, "input", false, true)
        .await
        .unwrap();
    set_attr(&cdp, "textarea", "data-target-changed", "yes").await;
    assert!(
        scoped
            .resolve(
                &mut cdp,
                key,
                local[0].range,
                "fill",
                Some("must not apply")
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_snapshot")
    );
    assert_eq!(textarea_value(&cdp).await, "scoped value");

    // Filling replaces the entire value, supports empty values and Unicode.
    for value in ["replacement 🌏\nsecond line", "", "last value"] {
        let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
        let range = snapshot
            .dom
            .find(&parser, "input", false, true)
            .await
            .unwrap()[0]
            .range;
        snapshot
            .resolve(&mut cdp, key, range, "fill", Some(value))
            .await
            .unwrap();
        assert_eq!(textarea_value(&cdp).await, value);
    }
    // Replacing a node with identical HTML must still invalidate its identity.
    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    let range = snapshot
        .dom
        .find(&parser, "input", false, true)
        .await
        .unwrap()[0]
        .range;
    let doc = cdp.command("DOM.getDocument", json!({})).await.unwrap();
    let id = cdp
        .command(
            "DOM.querySelector",
            json!({"nodeId":doc["root"]["nodeId"],"selector":"textarea"}),
        )
        .await
        .unwrap()["nodeId"]
        .clone();
    let body = cdp
        .command(
            "DOM.querySelector",
            json!({"nodeId":doc["root"]["nodeId"],"selector":"body"}),
        )
        .await
        .unwrap()["nodeId"]
        .clone();
    cdp.command(
        "DOM.copyTo",
        json!({"nodeId":id,"targetNodeId":body,"insertBeforeNodeId":id}),
    )
    .await
    .unwrap();
    cdp.command("DOM.removeNode", json!({"nodeId":id}))
        .await
        .unwrap();
    assert!(
        snapshot
            .resolve(&mut cdp, key, range, "fill", Some("must not apply"))
            .await
            .unwrap_err()
            .to_string()
            .contains("stale_snapshot")
    );

    let snapshot = Snapshot::take(&mut cdp, key).await.unwrap();
    cdp.navigate(&format!("{origin}/snapshot")).await.unwrap();
    assert!(
        snapshot
            .resolve(&mut cdp, key, range, "click", None)
            .await
            .is_err()
    );
    cdp.navigate(&format!("{origin}/snapshot#fragment"))
        .await
        .unwrap();
    assert_eq!(
        cdp.url().await.unwrap(),
        format!("{origin}/snapshot#fragment")
    );
    assert!(
        cdp.command("Runtime.evaluate", json!({"expression":"1"}))
            .await
            .is_err()
    );
    cdp.close().await.unwrap();
    println!(
        "PASS native snapshots, UTF-8 ranges, fill replacement, stale/detached nodes and navigation without JS evaluation"
    );
}
