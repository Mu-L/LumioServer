//! Host-owned opaque storage. Runtime supplies a committed cut, never network
//! traffic; this module owns neither ECS fields nor Voxel serialization.
//! Checkpoint publication groups both payloads in one immutable directory.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAX_CHECKPOINTS: usize = 4096;
const MAX_MANIFEST_BYTES: u64 = 16_384;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn durable_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut f = File::create_new(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

/// Explicit filesystem guarantee. Windows currently supports process-crash
/// checkpoints, not the stronger Unix directory-fsync power-loss claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageDurability {
    ProcessCrash,
    PowerLoss,
}
impl StorageDurability {
    fn sync_directory(self, path: &Path) -> io::Result<()> {
        if self == Self::PowerLoss {
            #[cfg(unix)]
            {
                File::open(path)?.sync_all()?;
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "power-loss directory synchronization unavailable",
                ));
            }
        }
        Ok(())
    }
}

/// Version/room identity is validated before returning any checkpoint bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreIdentity {
    pub room_id: String,
    pub release_id: String,
    pub contract_id: String,
    pub content_fingerprint: String,
}

/// One atomic Runtime/Voxel cut. The caller must obtain both at the same
/// committed barrier. `None` means the world has no Voxel participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub tick: u64,
    pub wal_sequence: u64,
    pub runtime: Vec<u8>,
    pub voxel: Option<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    generation: u64,
    identity: StoreIdentity,
    tick: u64,
    wal_sequence: u64,
    runtime_len: u64,
    runtime_hash: [u8; 32],
    voxel_len: Option<u64>,
    voxel_hash: Option<[u8; 32]>,
}

/// A single writer guarded by an OS file lock; old generations are never
/// overwritten. Retention is explicit and failure to prune is not hidden.
pub struct CheckpointStore {
    root: PathBuf,
    identity: StoreIdentity,
    max_bytes: usize,
    durability: StorageDurability,
    next_generation: u64,
    poisoned: bool,
    _writer_lock: File,
}
impl CheckpointStore {
    /// Opens a trusted storage directory. No destructive reset or migration.
    ///
    /// # Errors
    /// Rejects invalid bounds, another writer, inaccessible storage, or an
    /// unsupported durability profile.
    pub fn open(
        root: &Path,
        identity: StoreIdentity,
        max_bytes: usize,
        durability: StorageDurability,
    ) -> io::Result<Self> {
        if max_bytes == 0
            || max_bytes > 256 * 1024 * 1024
            || [
                &identity.room_id,
                &identity.release_id,
                &identity.contract_id,
                &identity.content_fingerprint,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256)
        {
            return Err(invalid("invalid checkpoint bounds or identity"));
        }
        fs::create_dir_all(root)?;
        if durability == StorageDurability::PowerLoss {
            let canonical = root.canonicalize()?;
            for directory in canonical.ancestors() {
                durability.sync_directory(directory)?;
            }
        }
        durability.sync_directory(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("writer.lock"))?;
        lock.try_lock()?;
        let mut store = Self {
            root: root.to_owned(),
            identity,
            max_bytes,
            durability,
            next_generation: 1,
            poisoned: false,
            _writer_lock: lock,
        };
        let existing = store.generations()?;
        if let Some((last, _)) = existing.last() {
            store.next_generation = last
                .checked_add(1)
                .ok_or_else(|| invalid("generation exhausted"))?;
        }
        Ok(store)
    }
    fn generations(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        let mut rows = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(number) = name.strip_prefix("checkpoint-") {
                if number.len() != 20 || !number.bytes().all(|c| c.is_ascii_digit()) {
                    // Ignore non-compliant directory names instead of permanently erroring
                    continue;
                }
                if !entry.file_type()?.is_dir() {
                    // Ignore non-directory items with checkpoint prefix
                    continue;
                }
                let Ok(generation) = number.parse() else {
                    continue;
                };
                rows.push((generation, entry.path()));
                if rows.len() > MAX_CHECKPOINTS {
                    return Err(invalid("checkpoint count exceeds retention bound"));
                }
            }
        }
        rows.sort_by_key(|(n, _)| *n);
        Ok(rows)
    }
    /// Publishes both participants together and returns only after the selected
    /// durability barrier. Any ambiguous I/O failure poisons this writer.
    ///
    /// # Errors
    /// Rejects bounds before I/O and propagates all write/sync/rename failures.
    pub fn publish(&mut self, checkpoint: &Checkpoint) -> io::Result<u64> {
        if self.poisoned {
            return Err(invalid(
                "checkpoint writer requires reopen after I/O failure",
            ));
        }
        let size = checkpoint
            .runtime
            .len()
            .checked_add(checkpoint.voxel.as_ref().map_or(0, Vec::len))
            .ok_or_else(|| invalid("checkpoint length overflow"))?;
        if checkpoint.runtime.is_empty() || size > self.max_bytes {
            return Err(invalid("checkpoint exceeds bounds"));
        }
        if self.generations()?.len() >= MAX_CHECKPOINTS {
            return Err(invalid("checkpoint retention exhausted"));
        }
        let generation = self.next_generation;
        let next = generation
            .checked_add(1)
            .ok_or_else(|| invalid("generation exhausted"))?;
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(io::Error::other)?;
        let draft = self
            .root
            .join(format!("draft-{:x}", u128::from_le_bytes(nonce)));
        let result = (|| {
            fs::create_dir(&draft)?;
            durable_write(&draft.join("runtime.bin"), &checkpoint.runtime)?;
            if let Some(bytes) = &checkpoint.voxel {
                durable_write(&draft.join("voxel.bin"), bytes)?;
            }
            let manifest = Manifest {
                version: 1,
                generation,
                identity: self.identity.clone(),
                tick: checkpoint.tick,
                wal_sequence: checkpoint.wal_sequence,
                runtime_len: checkpoint.runtime.len() as u64,
                runtime_hash: digest(&checkpoint.runtime),
                voxel_len: checkpoint.voxel.as_ref().map(|v| v.len() as u64),
                voxel_hash: checkpoint.voxel.as_ref().map(|v| digest(v)),
            };
            durable_write(
                &draft.join("manifest.json"),
                &serde_json::to_vec(&manifest)?,
            )?;
            self.durability.sync_directory(&draft)?;
            fs::rename(
                &draft,
                self.root.join(format!("checkpoint-{generation:020}")),
            )?;
            self.durability.sync_directory(&self.root)?;
            Ok(generation)
        })();
        if result.is_ok() {
            self.next_generation = next;
        } else {
            self.poisoned = true;
        }
        result
    }
    /// Returns the newest fully valid group. Corrupted groups are not mixed;
    /// fallback chooses the entire previous group. Identity mismatch is fatal.
    ///
    /// # Errors
    /// Returns an error if no published group is valid, or identity differs.
    pub fn recover(&self) -> io::Result<Option<Checkpoint>> {
        let rows = self.generations()?;
        if rows.is_empty() {
            return Ok(None);
        }
        for (generation, path) in rows.into_iter().rev() {
            let manifest: Manifest =
                match read_bounded(&path.join("manifest.json"), MAX_MANIFEST_BYTES)
                    .and_then(|v| serde_json::from_slice(&v).map_err(io::Error::other))
                {
                    Ok(m) => m,
                    Err(_) => continue,
                };
            if manifest.identity != self.identity {
                return Err(invalid("checkpoint release/room/content identity mismatch"));
            }
            if manifest.version != 1 || manifest.generation != generation {
                continue;
            }
            if manifest
                .runtime_len
                .saturating_add(manifest.voxel_len.unwrap_or(0))
                > self.max_bytes as u64
            {
                continue;
            }
            let result = (|| {
                let runtime = verified_bytes(
                    &path.join("runtime.bin"),
                    manifest.runtime_len,
                    manifest.runtime_hash,
                )?;
                let voxel = match (manifest.voxel_len, manifest.voxel_hash) {
                    (Some(len), Some(hash)) => {
                        Some(verified_bytes(&path.join("voxel.bin"), len, hash)?)
                    }
                    (None, None) => None,
                    _ => return Err(invalid("incomplete voxel metadata")),
                };
                Ok(Checkpoint {
                    tick: manifest.tick,
                    wal_sequence: manifest.wal_sequence,
                    runtime,
                    voxel,
                })
            })();
            if let Ok(value) = result {
                return Ok(Some(value));
            }
        }
        Err(invalid("no complete valid checkpoint group"))
    }
    /// Explicit bounded retention. Keeps at least two generations; run only
    /// after a successful recover/publish and independent backup policy.
    ///
    /// # Errors
    /// Invalid retention or filesystem errors are returned, never ignored.
    pub fn retain_latest(&mut self, keep: usize) -> io::Result<()> {
        if keep < 2 || self.poisoned {
            return Err(invalid("invalid retention or poisoned writer"));
        }
        let rows = self.generations()?;
        for (_, path) in rows.iter().take(rows.len().saturating_sub(keep)) {
            fs::remove_dir_all(path)?;
        }
        self.durability.sync_directory(&self.root)
    }
}
fn read_bounded(path: &Path, maximum: u64) -> io::Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid("storage entry must be a regular file"));
    }
    let file = File::open(path)?;
    if file.metadata()?.len() > maximum {
        return Err(invalid("storage object exceeds limit"));
    }
    let mut bytes = Vec::new();
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(invalid("storage object grew beyond limit"));
    }
    Ok(bytes)
}
fn verified_bytes(path: &Path, length: u64, hash: [u8; 32]) -> io::Result<Vec<u8>> {
    let bytes = read_bounded(path, length)?;
    if bytes.len() as u64 != length || digest(&bytes) != hash {
        return Err(invalid("checkpoint checksum mismatch"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> StoreIdentity {
        StoreIdentity {
            room_id: "r".into(),
            release_id: "v".into(),
            contract_id: "c".into(),
            content_fingerprint: "content".into(),
        }
    }
    fn point(tick: u64) -> Checkpoint {
        Checkpoint {
            tick,
            wal_sequence: tick,
            runtime: vec![1, u8::try_from(tick).expect("small test tick")],
            voxel: Some(vec![2, u8::try_from(tick).expect("small test tick")]),
        }
    }
    #[test]
    fn complete_groups_recover_together_and_partial_drafts_are_ignored() {
        let d = tempfile::tempdir().unwrap();
        let mut s =
            CheckpointStore::open(d.path(), identity(), 1024, StorageDurability::ProcessCrash)
                .unwrap();
        s.publish(&point(1)).unwrap();
        fs::create_dir(d.path().join("draft-interrupted")).unwrap();
        fs::write(d.path().join("draft-interrupted/runtime.bin"), b"partial").unwrap();
        assert_eq!(s.recover().unwrap(), Some(point(1)));
        s.publish(&point(2)).unwrap();
        fs::write(
            d.path().join("checkpoint-00000000000000000002/voxel.bin"),
            b"bad",
        )
        .unwrap();
        assert_eq!(s.recover().unwrap(), Some(point(1)));
    }
    #[test]
    fn second_writer_and_wrong_release_are_rejected() {
        let d = tempfile::tempdir().unwrap();
        let mut s =
            CheckpointStore::open(d.path(), identity(), 1024, StorageDurability::ProcessCrash)
                .unwrap();
        assert!(
            CheckpointStore::open(d.path(), identity(), 1024, StorageDurability::ProcessCrash)
                .is_err()
        );
        s.publish(&point(1)).unwrap();
        drop(s);
        let mut wrong = identity();
        wrong.release_id = "other".into();
        assert!(
            CheckpointStore::open(d.path(), wrong, 1024, StorageDurability::ProcessCrash)
                .unwrap()
                .recover()
                .is_err()
        );
    }
    #[test]
    fn oversize_is_rejected_before_publication() {
        let d = tempfile::tempdir().unwrap();
        let mut s = CheckpointStore::open(d.path(), identity(), 3, StorageDurability::ProcessCrash)
            .unwrap();
        assert!(s.publish(&point(1)).is_err());
        assert!(s.recover().unwrap().is_none());
    }
    #[test]
    fn non_compliant_checkpoint_directory_entries_are_ignored() {
        let d = tempfile::tempdir().unwrap();
        let mut s =
            CheckpointStore::open(d.path(), identity(), 1024, StorageDurability::ProcessCrash)
                .unwrap();
        s.publish(&point(1)).unwrap();
        // Create invalid checkpoint entries: non-numeric suffix, too short/long, or not a directory
        fs::create_dir(d.path().join("checkpoint-notanumber")).unwrap();
        fs::create_dir(d.path().join("checkpoint-123")).unwrap();
        fs::write(d.path().join("checkpoint-00000000000000000099"), b"file").unwrap();
        // The store must still list the valid generation and recover cleanly
        assert_eq!(s.recover().unwrap(), Some(point(1)));
    }
    #[test]
    fn power_loss_durability_behavior_matches_platform() {
        let d = tempfile::tempdir().unwrap();
        let res = CheckpointStore::open(d.path(), identity(), 1024, StorageDurability::PowerLoss);
        #[cfg(unix)]
        {
            let mut store = res.expect("Unix supports directory fsync for power loss");
            assert_eq!(store.publish(&point(1)).unwrap(), 1);
            assert_eq!(store.recover().unwrap(), Some(point(1)));
        }
        #[cfg(not(unix))]
        {
            assert!(res.is_err(), "Non-unix rejects power-loss fsync durability");
        }
    }
}
