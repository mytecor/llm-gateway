use super::{Element, Snapshot, range_key};
use anyhow::{Context, Result, ensure};
use universal_self_healing_parser::{Language, ParseRequest, Parser};
impl Snapshot {
    pub async fn find(
        &self,
        parser: &Parser,
        rule: &str,
        heal: bool,
        required: bool,
    ) -> Result<Vec<Element>> {
        let selected = [rule];
        let result = parser
            .parse(ParseRequest {
                language: Language::Html,
                content: self.html.as_bytes(),
                rules: Some(&selected),
                required: if required { &selected } else { &[] },
                heal,
                snapshot_id: Some(&self.id),
            })
            .await?;
        ensure!(
            result.snapshot_id.as_deref() == Some(&self.id) && result.content_hash == self.hash,
            "parser snapshot mismatch"
        );
        result
            .matches
            .into_iter()
            .map(|m| {
                let value = self.elements.get(&range_key(m.range)).with_context(|| {
                    format!(
                        "parser match is not an exact element range: {}:{}",
                        m.range.start, m.range.end
                    )
                })?;
                Ok(Element {
                    range: m.range,
                    text: value
                        .text
                        .clone()
                        .context("matched element exceeds snapshot text budget; narrow the rule")?,
                    visible: value.visible,
                    enabled: value.enabled,
                })
            })
            .collect()
    }
}
