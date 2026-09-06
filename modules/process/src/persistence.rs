//! Host-owned opaque storage. Runtime supplies a committed cut, never network
//! traffic; this module owns neither ECS fields nor Voxel serialization.
//! Checkpoint publication groups both payloads in one immutable directory.
//! Journal framing is internal storage metadata, not a public game protocol.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_CHECKPOINTS: usize = 4096;
const MAX_MANIFEST_BYTES: u64 = 16_384;
const HEADER_BYTES: usize = 88;
const MAGIC: &[u8; 4] = b"LW01";

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
                    return Err(invalid("invalid checkpoint generation name"));
                }
                if !entry.file_type()?.is_dir() {
                    return Err(invalid("checkpoint is not a directory"));
                }
                let generation = number
                    .parse()
                    .map_err(|_| invalid("invalid checkpoint generation"))?;
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

/// Journal entries are opaque committed change records supplied by Runtime.
/// `InputCommand` bytes are NOT a substitute for a committed change record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    pub sequence: u64,
    pub tick: u64,
    pub bytes: Vec<u8>,
}
/// Receipt is returned only after `sync_all`. No speculative durable watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableReceipt {
    pub sequence: u64,
    pub tick: u64,
}
pub struct Journal {
    file: File,
    last_sequence: u64,
    last_tick: u64,
    previous: [u8; 32],
    max_record: usize,
    max_file: u64,
    poisoned: bool,
}
impl Journal {
    /// Opens/revalidates a single-writer journal. An incomplete final write is
    /// truncated; a complete record with invalid framing/checksum is rejected.
    ///
    /// # Errors
    /// Lock, bounds, sequence, corruption and I/O errors are fatal.
    pub fn open(
        path: &Path,
        max_record: usize,
        max_file: u64,
    ) -> io::Result<(Self, Vec<JournalRecord>)> {
        if max_record == 0
            || max_record > 16 * 1024 * 1024
            || max_file > 512 * 1024 * 1024
            || max_file < HEADER_BYTES as u64
        {
            return Err(invalid("invalid journal bounds"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock()?;
        let length = file.metadata()?.len();
        if length > max_file {
            return Err(invalid("journal exceeds configured size"));
        }
        let mut offset = 0;
        let mut previous = [0; 32];
        let mut sequence = 0;
        let mut last_tick = 0;
        let mut rows = Vec::new();
        while offset < length {
            if length - offset < HEADER_BYTES as u64 {
                file.set_len(offset)?;
                file.sync_all()?;
                break;
            }
            let mut header = [0u8; HEADER_BYTES];
            file.read_exact(&mut header)?;
            if &header[..4] != MAGIC {
                return Err(invalid("journal magic mismatch"));
            }
            let size = u32::from_le_bytes(header[4..8].try_into().map_err(|_| invalid("length"))?)
                as usize;
            let seq =
                u64::from_le_bytes(header[8..16].try_into().map_err(|_| invalid("sequence"))?);
            let tick = u64::from_le_bytes(header[16..24].try_into().map_err(|_| invalid("tick"))?);
            if size > max_record
                || size == 0
                || seq != sequence + 1
                || tick < last_tick
                || header[24..56] != previous
            {
                return Err(invalid("journal metadata mismatch"));
            }
            if length - offset - (HEADER_BYTES as u64) < size as u64 {
                file.set_len(offset)?;
                file.sync_all()?;
                break;
            }
            let mut body = vec![0u8; size];
            file.read_exact(&mut body)?;
            let mut hash = Sha256::new();
            hash.update(&header[..56]);
            hash.update(&body);
            let expected: [u8; 32] = hash.finalize().into();
            if header[56..88] != expected {
                return Err(invalid("journal checksum mismatch"));
            }
            previous = expected;
            sequence = seq;
            last_tick = tick;
            rows.push(JournalRecord {
                sequence: seq,
                tick,
                bytes: body,
            });
            offset += HEADER_BYTES as u64 + size as u64;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                file,
                last_sequence: sequence,
                last_tick,
                previous,
                max_record,
                max_file,
                poisoned: false,
            },
            rows,
        ))
    }
    /// Appends one committed cut and synchronizes before acknowledging it.
    ///
    /// # Errors
    /// Capacity/order rejection leaves the file unchanged. An I/O failure
    /// poisons the writer because the durable outcome is then ambiguous.
    pub fn append(&mut self, tick: u64, bytes: &[u8]) -> io::Result<DurableReceipt> {
        if self.poisoned {
            return Err(invalid("journal writer requires reopen"));
        }
        if bytes.is_empty() || bytes.len() > self.max_record || tick < self.last_tick {
            return Err(invalid("invalid journal record"));
        }
        let sequence = self
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("journal sequence exhausted"))?;
        if self
            .file
            .metadata()?
            .len()
            .saturating_add(HEADER_BYTES as u64 + bytes.len() as u64)
            > self.max_file
        {
            return Err(invalid("journal rotation required"));
        }
        let mut header = [0u8; HEADER_BYTES];
        header[..4].copy_from_slice(MAGIC);
        header[4..8].copy_from_slice(
            &u32::try_from(bytes.len())
                .map_err(|_| invalid("length overflow"))?
                .to_le_bytes(),
        );
        header[8..16].copy_from_slice(&sequence.to_le_bytes());
        header[16..24].copy_from_slice(&tick.to_le_bytes());
        header[24..56].copy_from_slice(&self.previous);
        let mut hash = Sha256::new();
        hash.update(&header[..56]);
        hash.update(bytes);
        let checksum: [u8; 32] = hash.finalize().into();
        header[56..88].copy_from_slice(&checksum);
        self.poisoned = true;
        self.file.write_all(&header)?;
        self.file.write_all(bytes)?;
        self.file.sync_all()?;
        self.previous = checksum;
        self.last_sequence = sequence;
        self.last_tick = tick;
        self.poisoned = false;
        Ok(DurableReceipt { sequence, tick })
    }
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
    fn journal_truncated_tail_recovers_but_full_corruption_does_not() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("wal");
        let (mut w, _) = Journal::open(&p, 1024, 4096).unwrap();
        assert_eq!(
            w.append(1, b"one").unwrap(),
            DurableReceipt {
                sequence: 1,
                tick: 1
            }
        );
        w.append(2, b"two").unwrap();
        drop(w);
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(f.metadata().unwrap().len() - 1).unwrap();
        drop(f);
        let (mut w, rows) = Journal::open(&p, 1024, 4096).unwrap();
        assert_eq!(rows.len(), 1);
        w.append(2, b"two").unwrap();
        drop(w);
        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(HEADER_BYTES as u64)).unwrap();
        f.write_all(b"BAD").unwrap();
        drop(f);
        assert!(Journal::open(&p, 1024, 4096).is_err());
    }
    #[test]
    fn journal_capacity_and_sequence_are_explicit() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("wal");
        let (mut w, _) = Journal::open(&p, 8, 100).unwrap();
        assert!(w.append(1, b"ninebytes").is_err());
        assert_eq!(w.append(2, b"ok").unwrap().sequence, 1);
        assert!(w.append(1, b"late").is_err());
        assert!(w.append(3, b"full").is_err());
    }
}
