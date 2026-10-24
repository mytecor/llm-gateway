use anyhow::{Context, Result};
use serde::Deserialize;
// Columnar wire format returned by DOMSnapshot.captureSnapshot. All string
// references point into one response-local table; backend IDs identify real nodes.
#[derive(Deserialize)]
pub(crate) struct Capture {
    pub(super) strings: Vec<String>,
    pub(super) documents: Vec<Document>,
}
impl Capture {
    pub(crate) fn url(&self) -> Result<&str> {
        let document = self
            .documents
            .first()
            .context("snapshot document missing")?;
        self.string(document.document_url)
    }

    pub(super) fn string(&self, index: i64) -> Result<&str> {
        if index == -1 {
            return Ok("");
        }
        self.strings
            .get(usize::try_from(index).context("invalid snapshot string index")?)
            .map(String::as_str)
            .context("invalid snapshot string index")
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Document {
    #[serde(rename = "documentURL")]
    pub(super) document_url: i64,
    pub(super) nodes: Nodes,
    pub(super) layout: Layout,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Nodes {
    pub(super) parent_index: Vec<i64>,
    pub(super) node_type: Vec<i64>,
    pub(super) node_name: Vec<i64>,
    pub(super) node_value: Vec<i64>,
    pub(super) backend_node_id: Vec<i64>,
    pub(super) attributes: Vec<Vec<i64>>,
    #[serde(default)]
    pub(super) text_value: RareStrings,
    #[serde(default)]
    pub(super) input_value: RareStrings,
    #[serde(default)]
    pub(super) shadow_root_type: RareStrings,
    #[serde(default)]
    pub(super) pseudo_type: RareStrings,
}
#[derive(Default, Deserialize)]
pub(super) struct RareStrings {
    #[serde(default)]
    pub(super) index: Vec<usize>,
    #[serde(default)]
    pub(super) value: Vec<i64>,
}
impl RareStrings {
    pub(super) fn get<'a>(&self, index: usize, capture: &'a Capture) -> Result<Option<&'a str>> {
        self.index
            .iter()
            .position(|i| *i == index)
            .map(|i| capture.string(self.value[i]))
            .transpose()
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Layout {
    pub(super) node_index: Vec<usize>,
    pub(super) styles: Vec<Vec<i64>>,
    pub(super) bounds: Vec<Vec<f64>>,
    pub(super) text: Vec<i64>,
}
