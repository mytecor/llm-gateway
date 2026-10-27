use crate::store::CompiledRule;
use crate::{Capture, Error, Language, Limits, Match, Result, SourceRange};
use ast_grep_core::{
    AstGrep, Node, matcher::MatcherExt, meta_var::MetaVariable, tree_sitter::StrDoc,
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
pub(crate) type Ast = AstGrep<StrDoc<Language>>;
pub(crate) type AstNode<'a> = Node<'a, StrDoc<Language>>;

pub(crate) struct Budget {
    start: Instant,
    duration: Duration,
}
impl Budget {
    pub fn new(limits: &Limits) -> Self {
        Self {
            start: Instant::now(),
            duration: limits.execution_timeout,
        }
    }
    pub fn check(&self) -> Result<()> {
        if self.start.elapsed() >= self.duration {
            Err(Error::Limit("execution timeout"))
        } else {
            Ok(())
        }
    }
}
pub(crate) fn parse(
    content: &str,
    language: Language,
    limits: &Limits,
    budget: &Budget,
) -> Result<Ast> {
    if content.len() > limits.max_content_bytes {
        return Err(Error::Limit("content bytes"));
    }
    budget.check()?;
    let ast = AstGrep::try_new(content, language).map_err(Error::Parse)?;
    for (count, node) in ast.root().dfs().enumerate() {
        budget.check()?;
        if count >= limits.max_ast_nodes {
            return Err(Error::Limit("AST nodes"));
        }
        if node.is_error() || node.is_missing() {
            return Err(Error::Parse(format!(
                "syntax error at byte {}",
                node.range().start
            )));
        }
    }
    Ok(ast)
}
pub(crate) fn execute(
    rule: &CompiledRule,
    ast: &Ast,
    limits: &Limits,
    budget: &Budget,
) -> Result<Vec<Match>> {
    let mut matches = Vec::new();
    for node in ast.root().dfs() {
        budget.check()?;
        let Some(found) = rule.config.matcher.match_node(node) else {
            continue;
        };
        if matches.len() >= limits.max_matches {
            return Err(Error::Limit("matches"));
        }
        let env = found.get_env();
        let mut captures = BTreeMap::new();
        for var in env.get_matched_variables() {
            let (name, nodes) = match var {
                MetaVariable::Capture(name, _) => {
                    let nodes = env
                        .get_match(&name)
                        .cloned()
                        .into_iter()
                        .collect::<Vec<_>>();
                    (name, nodes)
                }
                MetaVariable::MultiCapture(name) => {
                    let nodes = env.get_multiple_matches(&name);
                    (name, nodes)
                }
                _ => continue,
            };
            captures.insert(
                name.clone(),
                nodes
                    .iter()
                    .map(|node| Capture {
                        name: name.clone(),
                        kind: node.kind().into_owned(),
                        text: node.text().into_owned(),
                        range: range(node),
                    })
                    .collect(),
            );
        }
        let node = found.get_node();
        matches.push(Match {
            rule_id: rule.config.id.clone(),
            kind: node.kind().into_owned(),
            text: node.text().into_owned(),
            range: range(node),
            captures,
        });
    }
    Ok(matches)
}
pub(crate) fn range(node: &AstNode<'_>) -> SourceRange {
    let r = node.range();
    SourceRange {
        start: r.start,
        end: r.end,
    }
}
