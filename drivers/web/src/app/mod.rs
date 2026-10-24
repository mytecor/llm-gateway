//! Process entry point: CLI, account state and protocol dispatch.
mod cli;
mod serve;
pub(crate) mod state;
use crate::{
    browser::Page,
    protocol::emit,
    site::{Engine, profile::Profile},
};
use anyhow::Result;
use clap::Parser;
use cli::{Args, Command};
use serde_json::json;
use serve::{serve, serve_stdio};
use state::{account_dir, lock_account};
use std::collections::BTreeMap;
async fn execute(command: Option<Command>) -> Result<()> {
    let Some(command) = command else {
        let work = serve_stdio();
        tokio::pin!(work);
        return tokio::select! {
            result = &mut work => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        };
    };
    if let Command::Check { profile } = command {
        let p = Profile::load(&profile)?;
        p.parser(None, false).await?;
        eprintln!("Profile {} is valid.", p.site);
        return Ok(());
    }
    let exclusive = matches!(command, Command::Login(_) | Command::Clean(_));
    let options = match &command {
        Command::Login(o) | Command::Run(o) | Command::Clean(o) => o,
        _ => unreachable!(),
    };
    let profile = Profile::load(&options.profile)?;
    let dir = account_dir(options, &profile.site)?;
    let _lock = lock_account(&dir, exclusive)?;
    // Build/validate rules before connecting or opening a browser tab.
    profile.parser(None, false).await?;
    let headers: BTreeMap<_, _> = options.cdp_header_env.iter().cloned().collect();
    let page = Page::connect(&options.cdp_url, &headers, &profile.origin).await?;
    let mut engine = Engine::new(profile, page, &dir, options.heal).await?;
    let result = {
        let work = async {
            match command {
                Command::Login(_) => engine.login().await,
                Command::Clean(_) => engine.run("clean_system_files").await,
                Command::Run(_) => serve(&mut engine).await,
                _ => unreachable!(),
            }
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    };
    let closed = engine.page.close().await;
    result.and(closed)
}
pub async fn run() {
    let args = Args::parse();
    let wire = args.command.is_none() || matches!(args.command, Some(Command::Run(_)));
    if let Err(error) = execute(args.command).await {
        if wire {
            let _ = emit(json!({"type":"error","error":error.to_string()}));
        } else {
            eprintln!("{error:#}");
        }
        std::process::exit(1);
    }
}
