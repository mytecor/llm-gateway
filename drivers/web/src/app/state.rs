use super::cli::Options;
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};
pub(super) fn account_dir(options: &Options, site: &str) -> Result<PathBuf> {
    ensure!(
        !options.account.is_empty()
            && options
                .account
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "invalid account name"
    );
    let root = match &options.state_dir {
        Some(p) => p.clone(),
        None => PathBuf::from(std::env::var("HOME")?).join(".local/state/llm-gateway/web"),
    };
    let dir = root.join(site).join(&options.account);
    std::fs::create_dir_all(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}
pub(crate) fn lock_account(dir: &std::path::Path, exclusive: bool) -> Result<File> {
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join("account.lock"))?;
    if exclusive {
        f.try_lock_exclusive()
    } else {
        FileExt::try_lock_shared(&f)
    }
    .context("account is busy: login/cleanup requires no active gateway sessions")?;
    Ok(f)
}
