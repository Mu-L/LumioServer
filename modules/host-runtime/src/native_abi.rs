//! The workspace's single cross-platform loader for the Lumio Engine native
//! SDK.
//!
//! One process loads the native library once and reads one root table
//! (`lumio_engine_get_api_v1`). Both consumers hang off this module: the
//! `CoreCLR` host slots (`modules/process`) and the `NativeCore` `timer_*`
//! slots ([`crate::NativeAbiKernel`]). Layout truth is the architecture repo's
//! `engine/abi/native-abi.json`.
//!
//! Loading goes through `libloading` on every platform; there is no
//! Windows-only path and no second root table (Owner ruling D24).
#![allow(unsafe_code)] // FFI boundary: unsafe is the point of this module.

use std::ffi::c_void;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

/// ABI version requested from `lumio_engine_get_api_v1`.
pub const ABI_VERSION: u32 = 1;
/// The only exported SDK symbol; everything else hangs off the root table.
pub const ENTRY_SYMBOL: &str = "lumio_engine_get_api_v1";

/// Timer handle as passed across the ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerHandleAbi {
    /// Slot index within the owning manager.
    pub index: u32,
    /// Generation guarding against stale handles.
    pub generation: u32,
    /// Caller-supplied context word.
    pub context: u64,
}

impl TimerHandleAbi {
    /// The all-zero handle used as scratch storage for out-parameters.
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            index: 0,
            generation: 0,
            context: 0,
        }
    }
}

/// One fired-timer row returned by `timer_drain`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerDrainRecord {
    /// Handle slot index.
    pub handle_index: u32,
    /// Handle generation.
    pub handle_generation: u32,
    /// Handle context word.
    pub handle_context: u64,
    /// Due stamp (ms for wall clock, tick number for tick frame).
    pub due: u64,
    /// Monotonic schedule sequence, for stable ordering.
    pub schedule_sequence: u64,
    /// Dispatch id of the slot the timer was bound to.
    pub slot_dispatch_id: u32,
    /// Explicit tail padding, part of the ABI layout.
    pub pad: u32,
}

impl TimerDrainRecord {
    /// The all-zero row used to size drain buffers.
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            handle_index: 0,
            handle_generation: 0,
            handle_context: 0,
            due: 0,
            schedule_sequence: 0,
            slot_dispatch_id: 0,
            pad: 0,
        }
    }
}

/// Root API table returned by `lumio_engine_get_api_v1`.
///
/// x64 golden layout from `engine/abi/native-abi.json`: the `CoreCLR` prefix is
/// 88 bytes (`ping` at 56, the CLR chain at 64/72/80), followed by the
/// `NativeCore` `timer_*` slots. A library that publishes a shorter table
/// leaves the tail slots `None` — see [`NativeLibrary::open`].
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RootApiV1 {
    /// ABI version the library was built for.
    pub abi_version: u32,
    /// `size_of::<RootApiV1>()` as seen by the library.
    pub struct_size: u32,
    /// Lowercase-hex SHA-256 of the ABI definition.
    pub abi_hash: [u8; 32],
    /// Build identifier bytes.
    pub build_id: [u8; 16],
    /// Liveness probe: writes 1 into `*mut u32` marker, returns status.
    pub ping: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    /// Creates the `CoreCLR` host; resolves the managed entry fail-fast.
    pub create_clr_host: Option<
        unsafe extern "C" fn(
            *const u8,        // hostfxr_path (UTF-8 NUL-terminated)
            *const u8,        // runtime_config_path
            *const u8,        // assembly_path
            *const u8,        // entry_spec: '<assembly-qualified type>;<method>'
            *mut *mut c_void, // out opaque handle
        ) -> i32,
    >,
    /// One byte-protocol call into the managed entry.
    pub clr_host_call: Option<
        unsafe extern "C" fn(
            *mut c_void, // host
            *const u8,   // input (null allowed when input_len is 0)
            u32,         // input_len
            *mut u8,     // output (null allowed when capacity is 0)
            u32,         // output_capacity
            *mut u32,    // out bytes_written
        ) -> i32,
    >,
    /// Destroys the `CoreCLR` host.
    pub destroy_clr_host: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    /// Creates a timer manager for one [`crate::TimerMode`].
    pub timer_create_manager: Option<unsafe extern "C" fn(u32, *mut *mut c_void) -> i32>,
    /// Destroys a timer manager.
    pub timer_destroy_manager: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    /// Registers a dispatch id with a manager.
    pub timer_register_dispatch: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
    /// Registers a scope, returning its generation.
    pub timer_register_scope: Option<unsafe extern "C" fn(*mut c_void, u64, u32, *mut u32) -> i32>,
    /// Tears a scope down, cancelling everything inside it.
    pub timer_teardown_scope: Option<unsafe extern "C" fn(*mut c_void, u64) -> i32>,
    /// Creates a slot the scheduler can bind timers to.
    pub timer_create_slot: Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> i32>,
    /// Binds a slot to a dispatch id.
    pub timer_bind_slot: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, u32) -> i32>,
    /// Closes a slot.
    pub timer_close_slot: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
    /// Schedules a one-shot timer.
    pub timer_schedule_one_shot: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            u32,
            u32,
            u64,
            *mut c_void,
            *mut TimerHandleAbi,
        ) -> i32,
    >,
    /// Schedules a repeating timer.
    pub timer_schedule_repeating: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            u32,
            u32,
            u64,
            u64,
            *mut c_void,
            *mut TimerHandleAbi,
        ) -> i32,
    >,
    /// Cancels a scheduled timer.
    pub timer_cancel: Option<unsafe extern "C" fn(*mut c_void, *const TimerHandleAbi) -> i32>,
    /// Advances tick-frame time.
    pub timer_advance: Option<unsafe extern "C" fn(*mut c_void, u64) -> i32>,
    /// Pumps wall-clock time.
    pub timer_pump: Option<unsafe extern "C" fn(*mut c_void, u64) -> i32>,
    /// Drains fired timers into a caller buffer.
    pub timer_drain:
        Option<unsafe extern "C" fn(*mut c_void, *mut TimerDrainRecord, u32, *mut u32) -> i32>,
}

impl RootApiV1 {
    /// An all-zero table: every optional slot reads back as `None`.
    ///
    /// `Option<fn(..)>` is null-pointer-optimised, so a zero word *is* `None`;
    /// this is the shape a short library table is widened into.
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            abi_version: 0,
            struct_size: 0,
            abi_hash: [0; 32],
            build_id: [0; 16],
            ping: None,
            create_clr_host: None,
            clr_host_call: None,
            destroy_clr_host: None,
            timer_create_manager: None,
            timer_destroy_manager: None,
            timer_register_dispatch: None,
            timer_register_scope: None,
            timer_teardown_scope: None,
            timer_create_slot: None,
            timer_bind_slot: None,
            timer_close_slot: None,
            timer_schedule_one_shot: None,
            timer_schedule_repeating: None,
            timer_cancel: None,
            timer_advance: None,
            timer_pump: None,
            timer_drain: None,
        }
    }
}

/// Header every published root table starts with.
#[repr(C)]
#[derive(Clone, Copy)]
struct RootApiHeader {
    abi_version: u32,
    struct_size: u32,
}

/// Bytes of the root table up to (excluding) the first `timer_*` slot: the
/// `CoreCLR` consumer needs exactly this much.
pub const CLR_SLOTS_SIZE: usize = std::mem::offset_of!(RootApiV1, timer_create_manager);

/// Bytes of the whole root table: the `NativeCore` timer consumer needs all of
/// it.
pub const TIMER_SLOTS_SIZE: usize = std::mem::size_of::<RootApiV1>();

/// Every way opening the native library can fail.
#[derive(Debug)]
pub enum NativeAbiError {
    /// The library file is not on disk.
    MissingFile(PathBuf),
    /// The platform loader refused the image (bad format, missing deps, ...).
    LibraryLoadFailed {
        /// Path we tried to load.
        path: PathBuf,
        /// Loader-reported detail.
        detail: String,
    },
    /// `lumio_engine_get_api_v1` is not exported.
    EntryMissing,
    /// The entry symbol rejected [`ABI_VERSION`].
    UnsupportedVersion(i32),
    /// The entry symbol returned success but a null table.
    NullRootTable,
}

impl Display for NativeAbiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingFile(path) => write!(f, "native SDK file missing: {}", path.display()),
            Self::LibraryLoadFailed { path, detail } => {
                write!(f, "loading {} failed: {detail}", path.display())
            }
            Self::EntryMissing => write!(f, "export `{ENTRY_SYMBOL}` not found in the library"),
            Self::UnsupportedVersion(status) => write!(
                f,
                "entry symbol rejected ABI version {ABI_VERSION} (status {status})"
            ),
            Self::NullRootTable => write!(f, "entry symbol returned a null API table"),
        }
    }
}

impl std::error::Error for NativeAbiError {}

type GetApiV1 = unsafe extern "C" fn(u32, *mut *const RootApiV1) -> i32;

/// A loaded native library plus the root table it published.
///
/// The table holds function pointers into the image, so the library is kept
/// alive for as long as the table is readable: dropping this unloads it.
pub struct NativeLibrary {
    // Field order is drop order: the table is plain data, the library is
    // unloaded last. The handle is never read again — holding it is the whole
    // point, because unloading would dangle every fn pointer in `root`.
    root: RootApiV1,
    _library: libloading::Library,
}

impl std::fmt::Debug for NativeLibrary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // The table is raw fn pointers; report the shape only.
        f.debug_struct("NativeLibrary")
            .field("abi_version", &self.root.abi_version)
            .field("struct_size", &self.root.struct_size)
            .finish_non_exhaustive()
    }
}

impl NativeLibrary {
    /// Loads `path` and fetches the root table via [`ENTRY_SYMBOL`].
    ///
    /// A library that publishes a shorter table than this build knows about is
    /// accepted: the missing tail slots read back as `None`, and each consumer
    /// decides whether the slots it needs are present.
    ///
    /// # Errors
    ///
    /// See [`NativeAbiError`].
    pub fn open(path: &Path) -> Result<Self, NativeAbiError> {
        if !path.is_file() {
            return Err(NativeAbiError::MissingFile(path.to_path_buf()));
        }
        // SAFETY: loading an image runs its initialisers, which is inherent to
        // dynamic loading; the path comes from deployment configuration.
        let library = unsafe { libloading::Library::new(path) }.map_err(|error| {
            NativeAbiError::LibraryLoadFailed {
                path: path.to_path_buf(),
                detail: error.to_string(),
            }
        })?;
        let root = {
            let mut symbol = ENTRY_SYMBOL.as_bytes().to_vec();
            symbol.push(0);
            // SAFETY: the SDK contract guarantees this export has the
            // `GetApiV1` signature; the symbol lives as long as `library`.
            let entry: libloading::Symbol<'_, GetApiV1> =
                unsafe { library.get(&symbol) }.map_err(|_| NativeAbiError::EntryMissing)?;
            let mut root_ptr: *const RootApiV1 = std::ptr::null();
            // SAFETY: `root_ptr` is writable storage for one pointer.
            let status = unsafe { entry(ABI_VERSION, std::ptr::from_mut(&mut root_ptr)) };
            if status != 0 {
                return Err(NativeAbiError::UnsupportedVersion(status));
            }
            if root_ptr.is_null() {
                return Err(NativeAbiError::NullRootTable);
            }
            // SAFETY: the contract keeps the table in static storage for the
            // life of the module, and it is at least `RootApiHeader` long.
            unsafe { read_root(root_ptr) }
        };
        Ok(Self {
            root,
            _library: library,
        })
    }

    /// The root table this library published.
    #[must_use]
    pub const fn root(&self) -> &RootApiV1 {
        &self.root
    }
}

// SAFETY: `libloading::Library` is Send + Sync, and the table is plain data
// whose fn pointers are only called through the owning consumer.
unsafe impl Send for NativeLibrary {}

/// Copies a published root table into a full-width [`RootApiV1`].
///
/// Only the bytes the library says it published are read; the remainder stays
/// zero, so newer slots this build knows about read back as `None`.
///
/// # Safety
///
/// `ptr` must point at a readable table of at least `struct_size` bytes (and at
/// least [`RootApiHeader`]), valid for the duration of the call.
unsafe fn read_root(ptr: *const RootApiV1) -> RootApiV1 {
    // SAFETY: the caller guarantees the header is readable.
    let header = unsafe { std::ptr::read_unaligned(ptr.cast::<RootApiHeader>()) };
    let published = header.struct_size as usize;
    let copy = published.clamp(std::mem::size_of::<RootApiHeader>(), TIMER_SLOTS_SIZE);
    let mut out = RootApiV1::zeroed();
    // SAFETY: `copy` never exceeds either the published table or our own
    // struct, and the two allocations cannot overlap (one is a local).
    unsafe {
        std::ptr::copy_nonoverlapping(
            ptr.cast::<u8>(),
            std::ptr::from_mut(&mut out).cast::<u8>(),
            copy,
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clr_prefix_keeps_the_golden_x64_layout() {
        // engine/abi/native-abi.json: ping at 56, the CLR chain at 64/72/80,
        // CoreCLR prefix 88 bytes. The timer slots extend past that.
        assert_eq!(CLR_SLOTS_SIZE, 88);
        assert_eq!(std::mem::offset_of!(RootApiV1, ping), 56);
        assert_eq!(std::mem::offset_of!(RootApiV1, create_clr_host), 64);
        assert_eq!(std::mem::offset_of!(RootApiV1, clr_host_call), 72);
        assert_eq!(std::mem::offset_of!(RootApiV1, destroy_clr_host), 80);
        const { assert!(TIMER_SLOTS_SIZE > CLR_SLOTS_SIZE) };
    }

    #[test]
    fn a_zeroed_table_reports_every_slot_absent() {
        let root = RootApiV1::zeroed();
        assert!(root.ping.is_none());
        assert!(root.destroy_clr_host.is_none());
        assert!(root.timer_create_manager.is_none());
        assert!(root.timer_drain.is_none());
    }

    #[test]
    fn a_short_published_table_leaves_the_tail_slots_none() {
        let mut source = RootApiV1::zeroed();
        source.abi_version = ABI_VERSION;
        // A library that only publishes the CoreCLR prefix.
        source.struct_size = u32::try_from(CLR_SLOTS_SIZE).expect("prefix size fits");
        source.abi_hash = [0xab; 32];
        source.timer_pump = Some(stub_pump);

        // SAFETY: `source` is a full-width, readable table.
        let copied = unsafe { read_root(std::ptr::from_ref(&source)) };

        assert_eq!(copied.abi_hash, [0xab; 32]);
        assert!(
            copied.timer_pump.is_none(),
            "slots past the published size must not be copied"
        );
    }

    #[test]
    fn a_full_published_table_copies_every_slot() {
        let mut source = RootApiV1::zeroed();
        source.abi_version = ABI_VERSION;
        source.struct_size = u32::try_from(TIMER_SLOTS_SIZE).expect("full size fits");
        source.timer_pump = Some(stub_pump);

        // SAFETY: `source` is a full-width, readable table.
        let copied = unsafe { read_root(std::ptr::from_ref(&source)) };

        assert!(copied.timer_pump.is_some());
    }

    #[test]
    fn a_bogus_published_size_never_reads_past_the_table() {
        let mut source = RootApiV1::zeroed();
        source.abi_version = ABI_VERSION;
        source.struct_size = u32::MAX;

        // SAFETY: `source` is a full-width, readable table; the clamp is what
        // keeps the copy inside it.
        let copied = unsafe { read_root(std::ptr::from_ref(&source)) };

        assert_eq!(copied.struct_size, u32::MAX);
        assert!(copied.timer_pump.is_none());
    }

    #[test]
    fn opening_a_missing_file_is_reported_without_touching_the_loader() {
        let error = NativeLibrary::open(Path::new("no-such-native-image.bin"))
            .expect_err("missing file must fail");
        assert!(matches!(error, NativeAbiError::MissingFile(_)), "{error}");
    }

    unsafe extern "C" fn stub_pump(_manager: *mut c_void, _now: u64) -> i32 {
        0
    }
}
