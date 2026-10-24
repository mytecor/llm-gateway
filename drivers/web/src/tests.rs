use crate::{
    app::state::lock_account,
    protocol::Input,
    site::{Delta, profile::Profile},
};
use serde_json::json;
use std::path::PathBuf;

#[tokio::test]
async fn muse_profile_compiles_with_native_parser() {
    let p =
        Profile::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sites/muse.yaml")).unwrap();
    let parser = p.parser(None, false).await.unwrap();
    let html = "<html><body><textarea>🌏</textarea><input type=\"email\"><button type=\"submit\" data-pel-click=\"chat_send_message\">Send</button></body></html>";
    let result = parser
        .parse(universal_self_healing_parser::ParseRequest {
            language: universal_self_healing_parser::Language::Html,
            content: html.as_bytes(),
            rules: Some(&["chat-input", "login-email", "send-button"]),
            required: &[],
            heal: false,
            snapshot_id: Some("test"),
        })
        .await
        .unwrap();
    assert_eq!(result.matches.len(), 3, "{result:?}");
    for m in result.matches {
        assert_eq!(&html[m.range.start..m.range.end], m.text);
    }
}
#[test]
fn utf8_delta_and_rewrites() {
    let mut d = Delta::default();
    assert_eq!(d.update("Привет 🌏").unwrap(), "Привет 🌏");
    assert_eq!(d.update("Привет 🌏!").unwrap(), "!");
    assert_eq!(d.update("Привет 🌏!").unwrap(), "");
    assert!(d.update("Новый текст").is_err());
}
#[test]
fn cleanup_cannot_overlap_gateway_session() {
    let dir = tempfile::tempdir().unwrap();
    let active = lock_account(dir.path(), false).unwrap();
    let other = lock_account(dir.path(), false).unwrap();
    assert!(lock_account(dir.path(), true).is_err());
    drop(active);
    drop(other);
    let cleanup = lock_account(dir.path(), true).unwrap();
    assert!(lock_account(dir.path(), false).is_err());
    drop(cleanup);
    assert!(lock_account(dir.path(), false).is_ok());
}
#[test]
fn unknown_rule_and_recursive_pipeline_rejected() {
    let source = include_str!("../sites/muse.yaml");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        source.replace("rule: send-button", "rule: missing-rule"),
    )
    .unwrap();
    assert!(
        Profile::load(&path)
            .unwrap_err()
            .to_string()
            .contains("unknown rule")
    );
    std::fs::write(
        &path,
        source.replace(
            "pipeline: delete_system_file",
            "pipeline: clean_system_files",
        ),
    )
    .unwrap();
    assert!(
        Profile::load(&path)
            .unwrap_err()
            .to_string()
            .contains("recursive pipeline")
    );
}

#[tokio::test]
async fn muse_cleanup_rules_require_menu_and_dialog_context() {
    use universal_self_healing_parser::{Language, ParseRequest};
    let p =
        Profile::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sites/muse.yaml")).unwrap();
    let parser = p.parser(None, false).await.unwrap();
    let html = r#"<html><body>
      <button type="submit">Unrelated form</button>
      <div role="menu"><div role="menuitem" class="destructive">Delete</div><div role="menuitem">Rename</div></div>
      <div role="dialog"><button class="destructive">Confirm</button></div>
      <main><button aria-haspopup="menu" data-testid="hatch-file-editor-more-actions">File actions</button></main>
      <button aria-haspopup="menu">Unrelated navigation menu</button>
    </body></html>"#;
    let result = parser
        .parse(ParseRequest {
            language: Language::Html,
            content: html.as_bytes(),
            rules: Some(&["delete-system-file", "confirm-delete", "system-file-menu"]),
            required: &[],
            heal: false,
            snapshot_id: None,
        })
        .await
        .unwrap();
    assert_eq!(result.matches.len(), 3, "{result:?}");
    assert!(
        result
            .matches
            .iter()
            .all(|m| !m.text.contains("Unrelated") && !m.text.contains("Rename"))
    );
}

#[tokio::test]
async fn muse_system_rows_exclude_folders_and_accept_live_login_fields() {
    use universal_self_healing_parser::{Language, ParseRequest};
    let profile =
        Profile::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sites/muse.yaml")).unwrap();
    let parser = profile.parser(None, false).await.unwrap();
    let html = r#"<html><body>
      <input aria-label="Mobile number or email" inputmode="email">
      <input aria-label="6-digit security code" autocomplete="one-time-code" class="sr-only">
      <div role="button" class="group flex cursor-pointer flex-col text-start"><div><span>MEMORY.md</span></div><span class="text-caption-1">Text</span></div>
      <div role="button" class="group flex cursor-pointer flex-col text-start"><div><span>memory</span></div><span class="text-caption-1">Folder</span></div>
    </body></html>"#;
    let result = parser
        .parse(ParseRequest {
            language: Language::Html,
            content: html.as_bytes(),
            rules: Some(&[
                "login-email",
                "login-code",
                "system-file-row",
                "system-files-loaded",
            ]),
            required: &[],
            heal: false,
            snapshot_id: None,
        })
        .await
        .unwrap();
    assert_eq!(
        result
            .matches
            .iter()
            .filter(|m| m.rule_id == "system-file-row")
            .count(),
        1
    );
    assert_eq!(
        result
            .matches
            .iter()
            .filter(|m| m.rule_id == "system-files-loaded")
            .count(),
        2
    );
    assert_eq!(
        result
            .matches
            .iter()
            .filter(|m| m.rule_id == "login-email" || m.rule_id == "login-code")
            .count(),
        2
    );
}

#[test]
fn start_accepts_gateway_null_tools_and_optional_tools() {
    for raw in [
        r#"{"type":"start","tools":null}"#,
        r#"{"type":"start","tools":[]}"#,
        r#"{"type":"start"}"#,
    ] {
        let message: Input = serde_json::from_str(raw).unwrap();
        match message {
            Input::Start { tools, .. } => assert!(tools.is_none_or(|v| v.is_empty())),
            _ => panic!("expected start"),
        }
    }
}

#[test]
fn action_scoping_rejects_context_dependent_rules() {
    let mut profile =
        Profile::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sites/muse.yaml")).unwrap();
    profile.rules.insert(
        "test".into(),
        json!({"language":"Html","rule":{"kind":"element","regex":"^<button"}}),
    );
    assert!(profile.element_local_rule("test"));
    for key in [
        "inside", "has", "follows", "precedes", "matches", "pattern", "nthChild",
    ] {
        profile.rules.get_mut("test").unwrap()["rule"][key] = json!({"kind":"element"});
        assert!(
            !profile.element_local_rule("test"),
            "unsafe scope for {key}"
        );
        profile.rules.get_mut("test").unwrap()["rule"]
            .as_object_mut()
            .unwrap()
            .remove(key);
    }
    profile.rules.get_mut("test").unwrap()["constraints"] = json!({});
    assert!(!profile.element_local_rule("test"));
}
