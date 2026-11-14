use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointState {
    pub retailer: String,
    pub category: String,
    pub next_offset: usize,
    pub pages_completed: usize,
    #[serde(default)]
    pub page_size: Option<usize>,
    pub extraction_run_id: Uuid,
    pub updated_at: DateTime<Utc>,
    pub completed: bool,
}

impl CheckpointState {
    #[must_use]
    pub fn is_valid_for_run(&self, category: &str, run_date: NaiveDate, page_size: usize) -> bool {
        self.category == category
            && crate::runner::business_date_lima(self.updated_at) == run_date
            && self.page_size.is_none_or(|ps| ps == page_size)
    }
}

pub struct CheckpointStore {
    checkpoints_dir: PathBuf,
}

/// Sanitizes a category/query slug so it is safe to use inside a checkpoint filename.
#[must_use]
pub fn sanitize_category(category: &str) -> String {
    let mut out = String::with_capacity(category.len());
    let mut prev_underscore = true;

    for ch in category.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            prev_underscore = false;
        } else if !prev_underscore {
            out.push('_');
            prev_underscore = true;
        }
    }

    while out.ends_with('_') {
        out.pop();
    }

    if out.is_empty() {
        "default".to_string()
    } else {
        out
    }
}

impl CheckpointStore {
    #[must_use]
    pub fn new(bronze_dir: impl AsRef<Path>) -> Self {
        Self {
            checkpoints_dir: bronze_dir.as_ref().join("checkpoints"),
        }
    }

    #[must_use]
    pub fn path_for(&self, retailer: &str, category: &str) -> PathBuf {
        let filename = format!(
            "{}_{}.json",
            sanitize_category(retailer),
            sanitize_category(category)
        );
        self.checkpoints_dir.join(filename)
    }

    pub fn hashed_path_for(&self, retailer: &str, category: &str) -> PathBuf {
        let digest = crate::dedup::compute_payload_hash(&format!("{retailer}\0{category}"));
        let short_hash = &digest[..8];
        let filename = format!(
            "{}_{}_{}.json",
            sanitize_category(retailer),
            sanitize_category(category),
            short_hash
        );
        self.checkpoints_dir.join(filename)
    }

    fn read_state(path: &Path) -> anyhow::Result<CheckpointState> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read checkpoint {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse checkpoint {}", path.display()))
    }

    pub fn resolve_path_for_category(
        &self,
        retailer: &str,
        category: &str,
    ) -> anyhow::Result<PathBuf> {
        let primary = self.path_for(retailer, category);
        if primary.exists() {
            let state = Self::read_state(&primary)?;
            if state.retailer == retailer && state.category == category {
                return Ok(primary);
            }
            return Ok(self.hashed_path_for(retailer, category));
        }
        Ok(primary)
    }

    pub fn load(&self, retailer: &str, category: &str) -> anyhow::Result<Option<CheckpointState>> {
        let path = self.resolve_path_for_category(retailer, category)?;
        if !path.exists() {
            return Ok(None);
        }
        Self::read_state(&path).map(Some)
    }

    pub fn save(&self, state: &CheckpointState) -> anyhow::Result<()> {
        let path = self.resolve_path_for_category(&state.retailer, &state.category)?;
        let bytes = serde_json::to_vec_pretty(state)?;
        crate::atomic_write(&path, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn sanitize_category_replaces_unsafe_characters() {
        assert_eq!(sanitize_category("lacteos"), "lacteos");
        assert_eq!(
            sanitize_category("/tottus-pe/Lácteos & Bebidas?cat=1"),
            "tottus_pe_L_cteos_Bebidas_cat_1"
        );
    }

    #[test]
    fn checkpoint_store_returns_none_when_absent() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        let loaded = store.load("plazavea", "lacteos").unwrap();
        assert_eq!(loaded, None);
    }

    #[test]
    fn checkpoint_store_saves_and_reloads_state_across_updates() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        let run_id = Uuid::new_v4();
        let now = Utc::now();

        let state = CheckpointState {
            retailer: "tottus".to_string(),
            category: "leche/evaporada".to_string(),
            next_offset: 40,
            pages_completed: 2,
            page_size: Some(20),
            extraction_run_id: run_id,
            updated_at: now,
            completed: false,
        };

        store.save(&state).unwrap();
        let expected_path = dir
            .path()
            .join("checkpoints")
            .join("tottus_leche_evaporada.json");
        assert!(expected_path.exists());
        assert_eq!(store.path_for("tottus", "leche/evaporada"), expected_path);

        let reloaded = store.load("tottus", "leche/evaporada").unwrap().unwrap();
        assert_eq!(reloaded, state);

        let updated = CheckpointState {
            next_offset: 60,
            pages_completed: 3,
            completed: true,
            ..state
        };
        store.save(&updated).unwrap();
        let reloaded2 = store.load("tottus", "leche/evaporada").unwrap().unwrap();
        assert_eq!(reloaded2, updated);
        assert!(reloaded2.completed);
    }

    #[test]
    fn checkpoint_store_disambiguates_colliding_category_slugs() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        let now = Utc::now();

        let slash_state = CheckpointState {
            retailer: "tottus".to_string(),
            category: "leche/entera".to_string(),
            next_offset: 20,
            pages_completed: 1,
            page_size: Some(20),
            extraction_run_id: Uuid::new_v4(),
            updated_at: now,
            completed: false,
        };
        let space_state = CheckpointState {
            retailer: "tottus".to_string(),
            category: "leche entera".to_string(),
            next_offset: 60,
            pages_completed: 3,
            page_size: Some(20),
            extraction_run_id: Uuid::new_v4(),
            updated_at: now,
            completed: true,
        };

        store.save(&slash_state).unwrap();
        assert_eq!(
            store.load("tottus", "leche entera").unwrap(),
            None,
            "unseen colliding category must not load the primary category's checkpoint"
        );

        store.save(&space_state).unwrap();

        let primary_path = store.path_for("tottus", "leche/entera");
        assert!(primary_path.exists());
        let hashed_path = store.hashed_path_for("tottus", "leche entera");
        assert_ne!(primary_path, hashed_path);
        assert!(hashed_path.exists());

        assert_eq!(
            store.load("tottus", "leche/entera").unwrap(),
            Some(slash_state.clone())
        );
        assert_eq!(
            store.load("tottus", "leche entera").unwrap(),
            Some(space_state.clone())
        );

        let updated_space = CheckpointState {
            next_offset: 80,
            pages_completed: 4,
            ..space_state
        };
        store.save(&updated_space).unwrap();
        assert_eq!(
            store.load("tottus", "leche/entera").unwrap(),
            Some(slash_state)
        );
        assert_eq!(
            store.load("tottus", "leche entera").unwrap(),
            Some(updated_space)
        );
    }

    #[test]
    fn checkpoint_state_defaults_missing_page_size_to_none() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        let legacy_json = r#"{
            "retailer": "plazavea",
            "category": "lacteos",
            "next_offset": 20,
            "pages_completed": 1,
            "extraction_run_id": "11111111-1111-4111-8111-111111111111",
            "updated_at": "2026-11-13T18:00:00Z",
            "completed": false
        }"#;
        let path = store.path_for("plazavea", "lacteos");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, legacy_json).unwrap();

        let loaded = store.load("plazavea", "lacteos").unwrap().unwrap();
        assert_eq!(loaded.page_size, None);
        assert_eq!(loaded.next_offset, 20);
    }

    #[test]
    fn checkpoint_is_valid_for_run_checks_date_category_and_page_size() {
        let now = Utc::now();
        let today = crate::runner::business_date_lima(now);
        let cp = CheckpointState {
            retailer: "metro".to_string(),
            category: "leche/entera".to_string(),
            next_offset: 20,
            pages_completed: 1,
            extraction_run_id: Uuid::new_v4(),
            updated_at: now,
            completed: false,
            page_size: Some(20),
        };

        // Matching run
        assert!(cp.is_valid_for_run("leche/entera", today, 20));

        // Different date (tomorrow)
        let tomorrow = today.succ_opt().unwrap();
        assert!(!cp.is_valid_for_run("leche/entera", tomorrow, 20));

        // Category collision (slug collision where store sanitized to same filename, but category string differs)
        assert!(!cp.is_valid_for_run("leche entera", today, 20));

        // Different page_size
        assert!(!cp.is_valid_for_run("leche/entera", today, 50));

        // Legacy JSON without page_size deserializes with page_size: None
        let legacy_json = format!(
            r#"{{
                "retailer": "metro",
                "category": "leche/entera",
                "next_offset": 20,
                "pages_completed": 1,
                "extraction_run_id": "{}",
                "updated_at": "{}",
                "completed": false
            }}"#,
            cp.extraction_run_id,
            cp.updated_at.to_rfc3339()
        );
        let legacy_cp: CheckpointState = serde_json::from_str(&legacy_json).unwrap();
        assert_eq!(legacy_cp.page_size, None);
        // Returns true for any page_size on the same date and category
        assert!(legacy_cp.is_valid_for_run("leche/entera", today, 20));
        assert!(legacy_cp.is_valid_for_run("leche/entera", today, 50));
        assert!(!legacy_cp.is_valid_for_run("leche/entera", tomorrow, 20));
        assert!(!legacy_cp.is_valid_for_run("leche entera", today, 20));
    }

    #[test]
    fn checkpoint_is_valid_for_run_uses_lima_business_date() {
        use chrono::TimeZone;
        let d = |y, m, day| NaiveDate::from_ymd_opt(y, m, day).unwrap();
        let mut cp = CheckpointState {
            retailer: "metro".to_string(),
            category: "lacteos".to_string(),
            next_offset: 20,
            pages_completed: 1,
            extraction_run_id: Uuid::new_v4(),
            // 20:00 Lima del 2026-11-14 == 01:00 UTC del 2026-11-15
            updated_at: Utc.with_ymd_and_hms(2026, 11, 15, 1, 0, 0).unwrap(),
            completed: false,
            page_size: Some(20),
        };
        assert!(cp.is_valid_for_run("lacteos", d(2026, 11, 14), 20));
        assert!(!cp.is_valid_for_run("lacteos", d(2026, 11, 15), 20));
        // 23:59:59 Lima del 2026-11-13 == 04:59:59 UTC del 2026-11-14
        cp.updated_at = Utc.with_ymd_and_hms(2026, 11, 14, 4, 59, 59).unwrap();
        assert!(cp.is_valid_for_run("lacteos", d(2026, 11, 13), 20));
        assert!(!cp.is_valid_for_run("lacteos", d(2026, 11, 14), 20));
    }
}
