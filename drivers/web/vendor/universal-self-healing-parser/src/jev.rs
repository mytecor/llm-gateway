use crate::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChoiceQuestion {
    #[serde(rename = "type")]
    pub question_type: String,
    pub instructions: String,
    pub criteria: BTreeMap<String, String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRequest {
    pub state: String,
    pub questions: BTreeMap<String, ChoiceQuestion>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChoiceAnswer {
    pub choice: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecisionResponse {
    pub answers: BTreeMap<String, ChoiceAnswer>,
}

/// One call per parse, no retries. Implementations must not log document context or credentials.
#[async_trait]
pub trait JevClient: Send + Sync {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResponse>;
}

/// Official TypeSafe System One HTTP API or compatible custom provider. Credentials are deliberately not Debug.
pub struct HttpJevClient {
    client: reqwest::Client,
    api_key: String,
    model: String,
    endpoint: String,
}
impl HttpJevClient {
    pub const DEFAULT_BASE_URL: &'static str = "https://api.typesafe.ai/v1/systemone";

    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| Error::Healing("could not create HTTP client".into()))?;
        let endpoint = std::env::var("JEV_BASE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| Self::normalize_url(&s))
            .unwrap_or_else(|| Self::DEFAULT_BASE_URL.to_string());
        Ok(Self {
            client,
            api_key: api_key.into(),
            model: "jev-latest".into(),
            endpoint,
        })
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Sets the base URL of the API provider.
    ///
    /// Accepts a host URL (e.g. `https://api.typesafe.ai`), a versioned base URL
    /// (e.g. `https://api.typesafe.ai/v1`), or a complete endpoint URL
    /// (e.g. `https://api.typesafe.ai/v1/systemone`).
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint = Self::normalize_url(&base_url.into());
        self
    }

    /// Sets the base URL of the API provider (alias for `base_url`).
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        self.base_url(base_url)
    }

    /// Sets the exact endpoint URL directly without path normalization.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into().trim().trim_end_matches('/').to_string();
        self
    }

    /// Sets the exact endpoint URL directly without path normalization (alias for `endpoint`).
    pub fn endpoint_url(self, endpoint_url: impl Into<String>) -> Self {
        self.endpoint(endpoint_url)
    }

    /// Returns the currently configured endpoint URL.
    pub fn current_endpoint(&self) -> &str {
        &self.endpoint
    }

    fn normalize_url(raw: &str) -> String {
        let trimmed = raw.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return Self::DEFAULT_BASE_URL.to_string();
        }
        if trimmed.ends_with("/v1/systemone") || trimmed.ends_with("/systemone") {
            trimmed.to_string()
        } else if trimmed.ends_with("/v1") {
            format!("{trimmed}/systemone")
        } else {
            format!("{trimmed}/v1/systemone")
        }
    }
}
#[async_trait]
impl JevClient for HttpJevClient {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResponse> {
        let body = serde_json::json!({ "model": self.model, "state": request.state, "questions": request.questions });
        let mut response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::Healing("Jev transport error".into()))?;
        if !response.status().is_success() {
            return Err(Error::Healing(format!(
                "Jev HTTP {}",
                response.status().as_u16()
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Healing("Jev response read error".into()))?
        {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                return Err(Error::Limit("Jev response bytes"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::Healing("invalid Jev response".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn test_normalize_url() {
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai/"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai/v1"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai/v1/"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai/v1/systemone"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("https://api.typesafe.ai/v1/systemone/"),
            "https://api.typesafe.ai/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("http://localhost:8080"),
            "http://localhost:8080/v1/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url("http://localhost:8080/custom/systemone"),
            "http://localhost:8080/custom/systemone"
        );
        assert_eq!(
            HttpJevClient::normalize_url(""),
            HttpJevClient::DEFAULT_BASE_URL
        );
    }

    #[test]
    fn test_client_builder_url_methods() {
        let client = HttpJevClient::new("dummy").unwrap();
        assert_eq!(client.current_endpoint(), HttpJevClient::DEFAULT_BASE_URL);

        let client = client.base_url("https://custom.provider.com");
        assert_eq!(
            client.current_endpoint(),
            "https://custom.provider.com/v1/systemone"
        );

        let client = client.endpoint("https://custom.provider.com/custom/path");
        assert_eq!(
            client.current_endpoint(),
            "https://custom.provider.com/custom/path"
        );
    }

    #[tokio::test]
    async fn test_custom_base_url_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]);

            assert!(req.starts_with("POST /v1/systemone HTTP/1.1"));
            assert!(req.contains("authorization: Bearer test_api_key"));

            let response_body = serde_json::json!({
                "answers": {
                    "rule1": { "choice": "option_a" }
                }
            })
            .to_string();

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let client = HttpJevClient::new("test_api_key")
            .unwrap()
            .base_url(format!("http://{addr}"));

        let res = client
            .decide(DecisionRequest {
                state: "test".into(),
                questions: BTreeMap::new(),
            })
            .await
            .unwrap();

        assert_eq!(res.answers["rule1"].choice, "option_a");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_custom_exact_endpoint_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]);

            assert!(req.starts_with("POST /custom-api/decision HTTP/1.1"));

            let response_body = serde_json::json!({ "answers": {} }).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let client = HttpJevClient::new("test_api_key")
            .unwrap()
            .endpoint(format!("http://{addr}/custom-api/decision"));

        let res = client
            .decide(DecisionRequest {
                state: "test".into(),
                questions: BTreeMap::new(),
            })
            .await
            .unwrap();

        assert!(res.answers.is_empty());
        server.await.unwrap();
    }
}
