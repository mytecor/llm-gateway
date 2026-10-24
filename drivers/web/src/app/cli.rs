use clap::{Parser as CliParser, Subcommand};
use std::path::PathBuf;
#[derive(CliParser)]
#[command(version, about = "YAML-driven CDP adapter for llm-gateway")]
pub(super) struct Args {
    #[command(subcommand)]
    pub(super) command: Command,
}
#[derive(Subcommand)]
pub(super) enum Command {
    Check {
        #[arg(long)]
        profile: PathBuf,
    },
    Login(Options),
    Run(Options),
    Clean(Options),
}
#[derive(clap::Args)]
pub(super) struct Options {
    #[arg(long)]
    pub(super) profile: PathBuf,
    #[arg(
        long,
        env = "WEB_CDP_URL",
        default_value = "http://browser.mytecor-homelab.local"
    )]
    pub(super) cdp_url: String,
    #[arg(long, default_value = "main")]
    pub(super) account: String,
    #[arg(long, env = "WEB_STATE_DIR")]
    pub(super) state_dir: Option<PathBuf>,
    /// Header name and environment variable name, e.g. Authorization=CDP_AUTHORIZATION.
    #[arg(long, value_parser = header)]
    pub(super) cdp_header_env: Vec<(String, String)>,
    #[arg(long)]
    pub(super) heal: bool,
}
fn header(value: &str) -> std::result::Result<(String, String), String> {
    let (name, variable) = value
        .split_once('=')
        .ok_or("expected Header=ENV_VARIABLE")?;
    if name.is_empty() || variable.is_empty() {
        return Err("expected Header=ENV_VARIABLE".into());
    }
    Ok((name.into(), variable.into()))
}
