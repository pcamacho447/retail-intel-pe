use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()));
    let rust_ingestor_dir = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.clone());

    let git_sha = resolve_git_sha(&manifest_dir, &rust_ingestor_dir);
    println!("cargo:rustc-env=INGESTOR_GIT_SHA={git_sha}");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=src");

    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--absolute-git-dir"])
        .current_dir(&manifest_dir)
        .output()
    {
        if output.status.success() {
            let git_dir_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !git_dir_str.is_empty() {
                let mut git_dir = PathBuf::from(git_dir_str);
                if git_dir.is_relative() {
                    git_dir = manifest_dir.join(git_dir);
                }
                for subpath in &["HEAD", "index", "logs/HEAD"] {
                    let candidate = git_dir.join(subpath);
                    if candidate.exists() {
                        println!("cargo:rerun-if-changed={}", candidate.display());
                    }
                }
            }
        }
    }
}

fn resolve_git_sha(manifest_dir: &Path, rust_ingestor_dir: &Path) -> String {
    let output = match Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .current_dir(manifest_dir)
        .output()
    {
        Ok(out) if out.status.success() => out,
        _ => return "nogit".to_string(),
    };

    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if sha.is_empty() {
        return "nogit".to_string();
    }

    let is_dirty = match Command::new("git")
        .args(["status", "--porcelain", "--"])
        .arg(rust_ingestor_dir)
        .current_dir(manifest_dir)
        .output()
    {
        Ok(out) if out.status.success() => !String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        _ => false,
    };

    if is_dirty {
        format!("{sha}-dirty")
    } else {
        sha
    }
}
