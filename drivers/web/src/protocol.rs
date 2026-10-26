//! Gateway JSON-line message contract and event writer.
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Write};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdp_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cdp_header_env: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub heal: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    #[serde(alias = "describe")]
    Init {
        #[serde(default, deserialize_with = "protocol_version")]
        #[allow(dead_code)]
        protocol_version: u32,
        #[serde(default)]
        id: String,
        #[serde(default)]
        tool_strategy: String,
        #[serde(default)]
        models_url: String,
        #[serde(default)]
        #[allow(dead_code)]
        has_session: bool,
        #[serde(default)]
        #[allow(dead_code)]
        request_timeout_ms: Option<i64>,
        #[serde(default)]
        config: Option<Value>,
    },
    Models {
        #[serde(default)]
        #[allow(dead_code)]
        config: Option<Value>,
    },
    Start {
        #[serde(default)]
        config: Option<Value>,
        #[serde(default)]
        tools: Option<Vec<serde_json::Value>>,
        #[serde(default)]
        resume_id: Option<String>,
        #[serde(default)]
        system: Option<String>,
    },
    Prompt {
        text: String,
    },
    ToolResult {
        id: String,
        content: String,
    },
}
pub const PROTOCOL_VERSION: u32 = 1;

fn protocol_version<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u32, D::Error> {
    let version = u32::deserialize(deserializer)?;
    if version != 0 && version != PROTOCOL_VERSION {
        return Err(serde::de::Error::custom(
            "unsupported driver protocol version",
        ));
    }
    Ok(version)
}

pub(crate) fn versioned(mut event: Value) -> Value {
    if event.get("type").and_then(Value::as_str) == Some("ready") {
        event["protocol_version"] = PROTOCOL_VERSION.into();
    }
    event
}

pub fn emit(event: Value) -> Result<()> {
    let event = versioned(event);
    let stdout = io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, &event)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    #[test]
    fn shared_gateway_session_fixtures_parse() {
        for line in include_str!("../../../driverproto/testdata/session-commands.ndjson").lines() {
            serde_json::from_str::<Input>(line).unwrap();
        }
    }

    #[test]
    fn ready_version_matches_shared_fixture() {
        let line = include_str!("../../../driverproto/testdata/events.ndjson")
            .lines()
            .next()
            .unwrap();
        let expected: Value = serde_json::from_str(line).unwrap();
        let mut without_version = expected.clone();
        without_version
            .as_object_mut()
            .unwrap()
            .remove("protocol_version");
        assert_eq!(versioned(without_version), expected);
    }

    #[test]
    fn rejects_unknown_protocol_version() {
        assert!(serde_json::from_str::<Input>(r#"{"type":"init","protocol_version":99}"#).is_err());
    }
}
