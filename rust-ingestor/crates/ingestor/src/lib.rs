use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use anyhow::Context;
use uuid::Uuid;

pub mod adapter;
pub mod adapters;
pub mod checkpoint;
pub mod dedup;
pub mod domain;
pub mod http;
pub mod metrics;
pub mod runner;
pub mod storage;

/// Atomically writes `contents` to `target` by creating parent directories,
/// writing to a sibling temporary file, syncing, and renaming into place.
pub(crate) fn atomic_write(target: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let parent = target
        .parent()
        .context("target path must have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create directory {}", parent.display()))?;

    let tmp_name = format!(
        ".{}.tmp.{}",
        target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file"),
        Uuid::new_v4()
    );
    let tmp_path = parent.join(tmp_name);

    let write_res = (|| -> anyhow::Result<()> {
        let mut file = File::create(&tmp_path)
            .with_context(|| format!("failed to create temp file {}", tmp_path.display()))?;
        file.write_all(contents)?;
        file.sync_all()?;
        Ok(())
    })();

    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    fs::rename(&tmp_path, target).with_context(|| {
        let _ = fs::remove_file(&tmp_path);
        format!(
            "failed to rename temp file {} to {}",
            tmp_path.display(),
            target.display()
        )
    })?;

    Ok(())
}
