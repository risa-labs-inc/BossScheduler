//! On-disk state: one JSON file, replaced atomically (write temp, fsync, rename).

use crate::job::{Job, RunRecord};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Run records kept per job; older ones are dropped.
pub const HISTORY_KEEP: usize = 50;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct State {
    pub version: u32,
    pub jobs: Vec<Job>,
    /// job id -> runs, oldest first. Kept for deleted one-shots too.
    #[serde(default)]
    pub history: BTreeMap<String, Vec<RunRecord>>,
}

impl State {
    pub fn load(path: &Path) -> Result<State, String> {
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                format!(
                    "state file {} is unreadable ({e}); refusing to start rather than \
                                      discard jobs. Fix or move it aside.",
                    path.display()
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(State {
                version: 1,
                ..Default::default()
            }),
            Err(e) => Err(format!("read {}: {e}", path.display())),
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        let tmp = path.with_extension("json.tmp");
        let write = || -> std::io::Result<()> {
            let mut f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?;
            f.write_all(&serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?)?;
            f.sync_all()?;
            fs::rename(&tmp, path)?;
            if let Some(dir) = path.parent() {
                fs::File::open(dir)?.sync_all()?;
            }
            Ok(())
        };
        write().map_err(|e| format!("save {}: {e}", path.display()))
    }

    pub fn job(&self, id: &str) -> Option<&Job> {
        self.jobs.iter().find(|j| j.spec.id == id)
    }

    pub fn job_mut(&mut self, id: &str) -> Option<&mut Job> {
        self.jobs.iter_mut().find(|j| j.spec.id == id)
    }

    pub fn push_run(&mut self, rec: RunRecord) {
        let runs = self.history.entry(rec.job_id.clone()).or_default();
        runs.push(rec);
        if runs.len() > HISTORY_KEEP {
            let drop = runs.len() - HISTORY_KEEP;
            runs.drain(..drop);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_no_temp_left() {
        let dir = std::env::temp_dir().join(format!("bsched-store-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("state.json");
        let mut s = State::load(&p).unwrap();
        assert!(s.jobs.is_empty());
        s.jobs.push(
            serde_json::from_value(serde_json::json!({"spec": {
            "id": "a", "name": "a", "command": ["true"],
            "schedule": {"kind": "at", "at": "2030-01-01T00:00:00Z"}}}))
            .unwrap(),
        );
        s.save(&p).unwrap();
        assert!(!p.with_extension("json.tmp").exists());
        assert_eq!(State::load(&p).unwrap().jobs[0].spec.id, "a");
        fs::write(&p, b"{not json").unwrap();
        assert!(State::load(&p).unwrap_err().contains("refusing to start"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
