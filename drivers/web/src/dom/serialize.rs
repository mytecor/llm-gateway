use super::{
    CapturedElement, ELEMENT_TEXT_LIMIT, HTML_LIMIT, SNAPSHOT_SEQUENCE, Snapshot, TEXT_LIMIT,
    digest,
    wire::{Capture, Document},
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::atomic::Ordering};
impl Snapshot {
    pub(crate) fn from_capture(capture: &Capture, key: &str, root: Option<i64>) -> Result<Self> {
        let document = capture
            .documents
            .first()
            .context("snapshot document missing")?;
        let mut builder = Builder::new(capture, document)?;
        let root_index = if let Some(root) = root {
            document
                .nodes
                .backend_node_id
                .iter()
                .position(|id| *id == root)
                .context("DOM resolution failed: stale_snapshot")?
        } else {
            document
                .nodes
                .node_type
                .iter()
                .enumerate()
                .find_map(|(i, kind)| {
                    (*kind == 1 && document.nodes.parent_index[i] == 0).then_some(i)
                })
                .context("snapshot HTML root missing")?
        };
        // Ancestors still control disabled/editable state for scoped snapshots.
        let mut ancestors = Vec::new();
        let mut parent = document.nodes.parent_index[root_index];
        while parent >= 0 {
            ancestors.push(parent as usize);
            parent = document.nodes.parent_index[parent as usize];
        }
        let mut disabled = false;
        let mut editable = false;
        for &index in ancestors.iter().rev() {
            let attrs = builder.attrs(index)?;
            if attrs.get("contenteditable").is_some_and(|v| *v != "false") {
                editable = true;
            }
            if attrs.get("contenteditable").is_some_and(|v| *v == "false") {
                editable = false;
            }
        }
        // :disabled includes disabled fieldset ancestry, except its first legend.
        let mut child = root_index;
        for &index in &ancestors {
            if builder.tag(index)? == "fieldset" && builder.attrs(index)?.contains_key("disabled") {
                let legend = builder.children[index]
                    .iter()
                    .find(|&&i| builder.tag(i).ok() == Some("legend".into()));
                if legend != Some(&child) {
                    disabled = true;
                }
            }
            child = index;
        }
        builder.visit(root_index, disabled, editable, 0)?;
        Ok(Self {
            id: format!(
                "{key}:{}",
                SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ),
            hash: digest(builder.html.as_bytes()),
            html: builder.html,
            document: document.nodes.backend_node_id[0],
            root: document.nodes.backend_node_id[root_index],
            elements: builder.elements,
        })
    }
}
struct Builder<'a> {
    capture: &'a Capture,
    document: &'a Document,
    children: Vec<Vec<usize>>,
    layout: Vec<Option<usize>>,
    html: String,
    elements: BTreeMap<String, CapturedElement>,
    text_bytes: usize,
}
impl<'a> Builder<'a> {
    fn new(capture: &'a Capture, document: &'a Document) -> Result<Self> {
        let n = document.nodes.node_type.len();
        ensure!(
            n > 0
                && document.nodes.parent_index.len() == n
                && document.nodes.node_name.len() == n
                && document.nodes.node_value.len() == n
                && document.nodes.backend_node_id.len() == n
                && document.nodes.attributes.len() == n,
            "invalid snapshot node columns"
        );
        let mut children = vec![vec![]; n];
        for (i, &p) in document.nodes.parent_index.iter().enumerate() {
            if p >= 0 {
                ensure!((p as usize) < i, "invalid snapshot parent index");
                children[p as usize].push(i);
            }
        }
        let mut layout = vec![None; n];
        for (i, &node) in document.layout.node_index.iter().enumerate() {
            ensure!(
                node < n
                    && i < document.layout.styles.len()
                    && i < document.layout.bounds.len()
                    && i < document.layout.text.len(),
                "invalid snapshot layout columns"
            );
            layout[node] = Some(i);
        }
        Ok(Self {
            capture,
            document,
            children,
            layout,
            html: String::new(),
            elements: BTreeMap::new(),
            text_bytes: 0,
        })
    }
    fn tag(&self, index: usize) -> Result<String> {
        Ok(self
            .capture
            .string(self.document.nodes.node_name[index])?
            .to_ascii_lowercase())
    }
    fn attrs(&self, index: usize) -> Result<BTreeMap<&'a str, &'a str>> {
        let attrs = &self.document.nodes.attributes[index];
        ensure!(attrs.len().is_multiple_of(2), "invalid snapshot attributes");
        attrs
            .chunks_exact(2)
            .map(|pair| Ok((self.capture.string(pair[0])?, self.capture.string(pair[1])?)))
            .collect()
    }
    fn write(&mut self, text: &str) -> Result<()> {
        ensure!(
            self.html.len() + text.len() <= HTML_LIMIT,
            "snapshot_too_large"
        );
        self.html.push_str(text);
        Ok(())
    }
    // Returns textContent and rendered text. Layout text is supplied by the
    // browser (including CSS whitespace processing); block boundaries add lines.
    fn visit(
        &mut self,
        index: usize,
        disabled_fieldset: bool,
        inherited_editable: bool,
        depth: usize,
    ) -> Result<(String, String)> {
        ensure!(depth <= 512, "snapshot DOM nesting exceeds limit");
        let nodes = &self.document.nodes;
        if nodes.node_type[index] == 3 {
            let raw = self.capture.string(nodes.node_value[index])?.to_string();
            self.write(&escape(&raw, false))?;
            let rendered = self.layout[index]
                .map(|i| self.capture.string(self.document.layout.text[i]))
                .transpose()?
                .unwrap_or("")
                .to_string();
            return Ok((raw, rendered));
        }
        if nodes.node_type[index] != 1
            || nodes.shadow_root_type.index.contains(&index)
            || nodes.pseudo_type.index.contains(&index)
        {
            return Ok(Default::default());
        }
        let tag = self.tag(index)?;
        if matches!(tag.as_str(), "script" | "style" | "noscript") {
            return Ok(Default::default());
        }
        let attrs = self.attrs(index)?;
        let editable = attrs
            .get("contenteditable")
            .map_or(inherited_editable, |value| *value != "false");
        let disabled = attrs.contains_key("disabled")
            && matches!(
                tag.as_str(),
                "button" | "input" | "textarea" | "select" | "option" | "optgroup" | "fieldset"
            );
        let parent = nodes.parent_index[index];
        let disabled_optgroup = tag == "option"
            && parent >= 0
            && self.tag(parent as usize)? == "optgroup"
            && self.attrs(parent as usize)?.contains_key("disabled");
        let disabled_by_fieldset = disabled_fieldset
            && matches!(
                tag.as_str(),
                "button" | "input" | "textarea" | "select" | "fieldset"
            );
        let enabled = !(disabled
            || disabled_optgroup
            || disabled_by_fieldset
            || attrs.get("aria-disabled") == Some(&"true"));
        let input_editable = tag == "textarea"
            || (tag == "input"
                && !matches!(
                    attrs.get("type").copied().unwrap_or("text"),
                    "button"
                        | "submit"
                        | "reset"
                        | "checkbox"
                        | "radio"
                        | "file"
                        | "hidden"
                        | "image"
                        | "range"
                        | "color"
                ));
        let start = self.html.len();
        self.write(&format!("<{tag}"))?;
        // Preserve the browser's attribute order: rules may match the serialized HTML.
        for pair in nodes.attributes[index].chunks_exact(2) {
            self.write(&format!(
                " {}=\"{}\"",
                self.capture.string(pair[0])?,
                escape(self.capture.string(pair[1])?, true)
            ))?;
        }
        // Explicitly close void tags so the HTML parser cannot include following
        // whitespace in an implicit element range. Ranges stay exact byte spans.
        let is_void = matches!(
            tag.as_str(),
            "area"
                | "base"
                | "br"
                | "col"
                | "embed"
                | "hr"
                | "img"
                | "input"
                | "link"
                | "meta"
                | "param"
                | "source"
                | "track"
                | "wbr"
        );
        self.write(if is_void { " />" } else { ">" })?;
        let mut raw = String::new();
        let mut rendered = String::new();
        if !is_void {
            let first_legend = self.children[index]
                .iter()
                .find(|&&i| self.tag(i).ok().as_deref() == Some("legend"))
                .copied();
            for child in self.children[index].clone() {
                let fieldset = disabled_fieldset
                    || (tag == "fieldset" && disabled && Some(child) != first_legend);
                let (child_raw, child_rendered) =
                    self.visit(child, fieldset, editable, depth + 1)?;
                raw.push_str(&child_raw);
                rendered.push_str(&child_rendered);
            }
            self.write(&format!("</{tag}>"))?;
        }
        let mut visible = false;
        let mut block = false;
        if let Some(i) = self.layout[index] {
            let style = &self.document.layout.styles[i];
            ensure!(style.len() == 3, "missing snapshot computed styles");
            let display = self.capture.string(style[0])?;
            let visibility = self.capture.string(style[1])?;
            visible = display != "none"
                && !matches!(visibility, "hidden" | "collapse")
                && self.document.layout.bounds[i].len() == 4;
            block = matches!(
                display,
                "block" | "flex" | "grid" | "list-item" | "table-row" | "flow-root"
            );
        }
        let text = if self.layout[index].is_some() {
            rendered.trim_matches('\n').to_string()
        } else {
            raw.clone()
        };
        let text = if text.len() <= ELEMENT_TEXT_LIMIT && self.text_bytes + text.len() <= TEXT_LIMIT
        {
            self.text_bytes += text.len();
            Some(text)
        } else {
            None
        };
        let value = nodes
            .input_value
            .get(index, self.capture)?
            .or(nodes.text_value.get(index, self.capture)?)
            .map(String::from);
        self.elements.insert(
            format!("{start}:{}", self.html.len()),
            CapturedElement {
                backend: nodes.backend_node_id[index],
                text,
                visible,
                enabled,
                editable: (input_editable || editable) && !attrs.contains_key("readonly"),
                value,
            },
        );
        if tag == "br" {
            rendered.push('\n');
        }
        if block && !rendered.is_empty() {
            rendered = format!("\n{}\n", rendered.trim_matches('\n'));
        }
        if !visible {
            rendered.clear();
        }
        Ok((raw, rendered))
    }
}
fn escape(text: &str, attribute: bool) -> String {
    let escaped = text
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    if attribute {
        escaped.replace('"', "&quot;")
    } else {
        escaped
    }
}
