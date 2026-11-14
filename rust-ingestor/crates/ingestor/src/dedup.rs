use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Computes the deterministic lowercase hexadecimal SHA-256 digest of a raw JSON payload.
#[must_use]
pub fn compute_payload_hash(raw_payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_payload.as_bytes());
    hex::encode(hasher.finalize())
}

fn strip_volatile_keys(value: &mut Value, volatile_keys: &[&str]) {
    match value {
        Value::Object(map) => {
            for key in volatile_keys {
                map.remove(*key);
            }
            for child in map.values_mut() {
                strip_volatile_keys(child, volatile_keys);
            }
        }
        Value::Array(arr) => {
            for child in arr {
                strip_volatile_keys(child, volatile_keys);
            }
        }
        _ => {}
    }
}

/// Computes the SHA-256 content hash after stripping volatile JSON keys at any depth
/// and serializing with canonical key ordering. Falls back to `compute_payload_hash`
/// when `raw_payload` is not valid JSON.
pub fn compute_content_hash(raw_payload: &str, volatile_keys: &[&str]) -> String {
    let Ok(mut parsed) = serde_json::from_str::<Value>(raw_payload) else {
        return compute_payload_hash(raw_payload);
    };
    if !volatile_keys.is_empty() {
        strip_volatile_keys(&mut parsed, volatile_keys);
    }
    match serde_json::to_string(&parsed) {
        Ok(canonical) => compute_payload_hash(&canonical),
        Err(_) => compute_payload_hash(raw_payload),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SeenHashesFile {
    seen_hashes: Vec<String>,
}

pub struct PayloadDeduplicator {
    index_path: PathBuf,
    seen: HashSet<String>,
}

impl PayloadDeduplicator {
    /// Opens or initializes `<bronze_dir>/blobs/seen_hashes.json`.
    pub fn open(bronze_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let index_path = bronze_dir.as_ref().join("blobs").join("seen_hashes.json");
        let seen = if index_path.exists() {
            let bytes = fs::read(&index_path).with_context(|| {
                format!("failed to read dedup index at {}", index_path.display())
            })?;
            let parsed: SeenHashesFile = serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse dedup index at {}", index_path.display())
            })?;
            parsed.seen_hashes.into_iter().collect()
        } else {
            HashSet::new()
        };

        Ok(Self { index_path, seen })
    }

    /// Returns `true` if `payload_hash` was already seen; otherwise records it and returns `false`.
    pub fn check_and_mark(&mut self, payload_hash: &str) -> bool {
        !self.seen.insert(payload_hash.to_string())
    }

    /// Atomically persists all seen hashes to `<bronze_dir>/blobs/seen_hashes.json`.
    /// Merges existing hashes on disk (if any) with in-memory seen hashes before writing.
    pub fn persist(&mut self) -> anyhow::Result<()> {
        if self.index_path.exists() {
            let bytes = match fs::read(&self.index_path) {
                Ok(b) => Some(b),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed to read dedup index at {}",
                            self.index_path.display()
                        )
                    })
                }
            };
            if let Some(bytes) = bytes {
                let parsed: SeenHashesFile = serde_json::from_slice(&bytes).with_context(|| {
                    format!(
                        "failed to parse dedup index at {}",
                        self.index_path.display()
                    )
                })?;
                self.seen.extend(parsed.seen_hashes);
            }
        }

        let mut hashes: Vec<String> = self.seen.iter().cloned().collect();
        hashes.sort();
        let payload = serde_json::to_vec_pretty(&SeenHashesFile {
            seen_hashes: hashes,
        })?;
        crate::atomic_write(&self.index_path, &payload)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    #[must_use]
    pub fn contains(&self, payload_hash: &str) -> bool {
        self.seen.contains(payload_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn compute_payload_hash_produces_deterministic_sha256_hex() {
        assert_eq!(
            compute_payload_hash(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            compute_payload_hash("hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            compute_payload_hash(r#"{"productId":"123"}"#),
            compute_payload_hash(r#"{"productId":"123"}"#)
        );
    }

    #[test]
    fn compute_content_hash_ignores_volatile_keys_and_key_order() {
        let keys = [
            "PriceToken",
            "PriceValidUntil",
            "CacheVersionUsedToCallCheckout",
        ];
        let a = r#"{"productId":"1","items":[{"itemId":"9","sellers":[{"commertialOffer":{"Price":4.9,"PriceToken":"aaa","PriceValidUntil":"2026-11-14T01:00:00Z","CacheVersionUsedToCallCheckout":"x_1"}}]}]}"#;
        let b = r#"{"items":[{"sellers":[{"commertialOffer":{"PriceToken":"bbb","Price":4.9,"CacheVersionUsedToCallCheckout":"x_2","PriceValidUntil":"2026-11-14T01:30:00Z"}}],"itemId":"9"}],"productId":"1"}"#;
        let c = r#"{"productId":"1","items":[{"itemId":"9","sellers":[{"commertialOffer":{"Price":5.1,"PriceToken":"aaa"}}]}]}"#;
        assert_eq!(
            compute_content_hash(a, &keys),
            compute_content_hash(b, &keys)
        );
        assert_ne!(
            compute_content_hash(a, &keys),
            compute_content_hash(c, &keys)
        );
        assert_eq!(
            compute_content_hash("<html>", &keys),
            compute_payload_hash("<html>")
        );
    }

    #[test]
    fn deduplicator_marks_duplicates_within_and_across_runs() {
        let dir = TempDir::new().unwrap();

        let mut dedup = PayloadDeduplicator::open(dir.path()).unwrap();
        assert!(!dedup.check_and_mark("hash_1"));
        assert!(dedup.check_and_mark("hash_1"));
        assert!(!dedup.check_and_mark("hash_2"));
        assert_eq!(dedup.len(), 2);
        dedup.persist().unwrap();

        let expected_file = dir.path().join("blobs").join("seen_hashes.json");
        assert!(expected_file.exists());

        // Second run reloads persisted hashes
        let mut dedup_run2 = PayloadDeduplicator::open(dir.path()).unwrap();
        assert_eq!(dedup_run2.len(), 2);
        assert!(dedup_run2.check_and_mark("hash_1"));
        assert!(dedup_run2.check_and_mark("hash_2"));
        assert!(!dedup_run2.check_and_mark("hash_3"));
        dedup_run2.persist().unwrap();

        let dedup_run3 = PayloadDeduplicator::open(dir.path()).unwrap();
        assert_eq!(dedup_run3.len(), 3);
    }

    #[test]
    fn deduplicator_persist_merges_concurrent_writes_without_data_loss() {
        let dir = TempDir::new().unwrap();

        let mut dedup_a = PayloadDeduplicator::open(dir.path()).unwrap();
        let mut dedup_b = PayloadDeduplicator::open(dir.path()).unwrap();

        assert!(!dedup_a.check_and_mark("hash_a"));
        dedup_a.persist().unwrap();

        assert!(!dedup_b.check_and_mark("hash_b"));
        dedup_b.persist().unwrap();

        let dedup_c = PayloadDeduplicator::open(dir.path()).unwrap();
        assert_eq!(dedup_c.len(), 2);
        assert!(dedup_c.contains("hash_a"));
        assert!(dedup_c.contains("hash_b"));
    }
}
