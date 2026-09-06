//! Bind `NativeCore` `timer_*` slots from the process-wide root table.
//!
//! The library itself is opened by [`crate::NativeLibrary`] (Owner ruling D24:
//! one loader, one root table); this module only drives the timer slots.
#![allow(unsafe_code, clippy::borrow_as_ptr)]

use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::ptr;

use crate::kernel::{KernelError, KernelFired, KernelHandle, KernelTimer, TimerMode};
use crate::native_abi::{
    NativeLibrary, RootApiV1, TimerDrainRecord, TimerHandleAbi, TIMER_SLOTS_SIZE,
};

const WALL_DISPATCH: u32 = 1;
const TICK_DISPATCH: u32 = 2;
const SCOPE_ID: u64 = 1;
const SCOPE_WORLD: u32 = 0;

struct Manager {
    ptr: *mut c_void,
    slot: *mut c_void,
    generation: u32,
}

/// `NativeCore` ABI timer adapter. Two managers: wallClock + tickFrame.
pub struct NativeAbiKernel {
    api: RootApiV1,
    wall: Manager,
    tick: Manager,
    // Field order is drop order: managers are destroyed in `Drop` before the
    // library that owns their code is unloaded here.
    _library: NativeLibrary,
}

unsafe impl Send for NativeAbiKernel {}

impl NativeAbiKernel {
    /// Loads `timer_*` from the engine native DLL. Missing slots are BLOCKED.
    ///
    /// # Errors
    ///
    /// Returns a human-readable BLOCKED reason.
    pub fn load(engine_native: &Path) -> Result<Self, String> {
        let library =
            NativeLibrary::open(engine_native).map_err(|error| format!("BLOCKED: {error}"))?;
        let api = *library.root();
        let min_size = u32::try_from(TIMER_SLOTS_SIZE).unwrap_or(u32::MAX);
        if api.struct_size < min_size {
            return Err(format!(
                "BLOCKED: native ABI struct_size {} missing timer slots (need {min_size})",
                api.struct_size
            ));
        }
        if api.timer_create_manager.is_none()
            || api.timer_schedule_one_shot.is_none()
            || api.timer_pump.is_none()
            || api.timer_advance.is_none()
            || api.timer_drain.is_none()
        {
            return Err("BLOCKED: native ABI timer slots are null".to_owned());
        }
        let wall = setup_manager(&api, TimerMode::WallClock, WALL_DISPATCH)?;
        let tick = setup_manager(&api, TimerMode::TickFrame, TICK_DISPATCH)?;
        Ok(Self {
            api,
            wall,
            tick,
            _library: library,
        })
    }

    fn manager(&self, mode: TimerMode) -> &Manager {
        match mode {
            TimerMode::WallClock => &self.wall,
            TimerMode::TickFrame => &self.tick,
        }
    }
}

impl Drop for NativeAbiKernel {
    fn drop(&mut self) {
        if let Some(destroy) = self.api.timer_destroy_manager {
            unsafe {
                let _ = destroy(self.wall.ptr);
                let _ = destroy(self.tick.ptr);
            }
        }
    }
}

impl KernelTimer for NativeAbiKernel {
    fn schedule_one_shot(
        &mut self,
        mode: TimerMode,
        due: u64,
        dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        let _ = dispatch_id;
        let manager = self.manager(mode);
        let schedule = self
            .api
            .timer_schedule_one_shot
            .ok_or_else(|| status(1, "schedule_one_shot"))?;
        let mut handle = TimerHandleAbi::zeroed();
        let rc = unsafe {
            schedule(
                manager.ptr,
                SCOPE_ID,
                SCOPE_WORLD,
                manager.generation,
                due,
                manager.slot,
                &mut handle,
            )
        };
        check(rc, "schedule_one_shot")?;
        Ok(KernelHandle {
            index: handle.index,
            generation: handle.generation,
            context: handle.context,
        })
    }

    fn schedule_repeating(
        &mut self,
        mode: TimerMode,
        first_due: u64,
        interval: u64,
        dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        let _ = dispatch_id;
        let manager = self.manager(mode);
        let schedule = self
            .api
            .timer_schedule_repeating
            .ok_or_else(|| status(1, "schedule_repeating"))?;
        let mut handle = TimerHandleAbi::zeroed();
        let rc = unsafe {
            schedule(
                manager.ptr,
                SCOPE_ID,
                SCOPE_WORLD,
                manager.generation,
                first_due,
                interval,
                manager.slot,
                &mut handle,
            )
        };
        check(rc, "schedule_repeating")?;
        Ok(KernelHandle {
            index: handle.index,
            generation: handle.generation,
            context: handle.context,
        })
    }

    fn cancel(&mut self, handle: KernelHandle) -> Result<(), KernelError> {
        let cancel = self.api.timer_cancel.ok_or_else(|| status(1, "cancel"))?;
        let abi = TimerHandleAbi {
            index: handle.index,
            generation: handle.generation,
            context: handle.context,
        };
        let rc = unsafe { cancel(self.wall.ptr, &abi) };
        if rc == 0 {
            return Ok(());
        }
        let rc_tick = unsafe { cancel(self.tick.ptr, &abi) };
        check(rc_tick, "cancel")
    }

    fn pump_wall_clock(&mut self, now_ms: u64) -> Result<Vec<KernelFired>, KernelError> {
        let pump = self.api.timer_pump.ok_or_else(|| status(1, "pump"))?;
        check(unsafe { pump(self.wall.ptr, now_ms) }, "pump")?;
        drain(&self.api, self.wall.ptr)
    }

    fn advance_tick_frame(&mut self, to_tick: u64) -> Result<Vec<KernelFired>, KernelError> {
        let advance = self.api.timer_advance.ok_or_else(|| status(1, "advance"))?;
        check(unsafe { advance(self.tick.ptr, to_tick) }, "advance")?;
        drain(&self.api, self.tick.ptr)
    }
}

fn setup_manager(api: &RootApiV1, mode: TimerMode, dispatch: u32) -> Result<Manager, String> {
    let create = api
        .timer_create_manager
        .ok_or("BLOCKED: timer_create_manager")?;
    let mut ptr = ptr::null_mut();
    let rc = unsafe { create(mode as u32, &mut ptr) };
    if rc != 0 || ptr.is_null() {
        return Err(format!("BLOCKED: timer_create_manager status {rc}"));
    }
    let register = api
        .timer_register_dispatch
        .ok_or("BLOCKED: timer_register_dispatch")?;
    let rc = unsafe { register(ptr, dispatch) };
    if rc != 0 {
        return Err(format!("BLOCKED: timer_register_dispatch status {rc}"));
    }
    let register_scope = api
        .timer_register_scope
        .ok_or("BLOCKED: timer_register_scope")?;
    let mut generation = 0_u32;
    let rc = unsafe { register_scope(ptr, SCOPE_ID, SCOPE_WORLD, &mut generation) };
    if rc != 0 {
        return Err(format!("BLOCKED: timer_register_scope status {rc}"));
    }
    let create_slot = api.timer_create_slot.ok_or("BLOCKED: timer_create_slot")?;
    let mut slot = ptr::null_mut();
    let rc = unsafe { create_slot(ptr, &mut slot) };
    if rc != 0 || slot.is_null() {
        return Err(format!("BLOCKED: timer_create_slot status {rc}"));
    }
    let bind = api.timer_bind_slot.ok_or("BLOCKED: timer_bind_slot")?;
    let rc = unsafe { bind(ptr, slot, dispatch) };
    if rc != 0 {
        return Err(format!("BLOCKED: timer_bind_slot status {rc}"));
    }
    Ok(Manager {
        ptr,
        slot,
        generation,
    })
}

fn drain(api: &RootApiV1, manager: *mut c_void) -> Result<Vec<KernelFired>, KernelError> {
    let drain = api.timer_drain.ok_or_else(|| status(1, "drain"))?;
    let mut count = 0_u32;
    let probe = unsafe { drain(manager, ptr::null_mut(), 0, &mut count) };
    if probe == 0 {
        return Ok(Vec::new());
    }
    if probe != 5 && count == 0 {
        return check(probe, "drain").map(|()| Vec::new());
    }
    let mut records = vec![TimerDrainRecord::zeroed(); count.max(1) as usize];
    let mut written = 0_u32;
    let rc = unsafe { drain(manager, records.as_mut_ptr(), count.max(1), &mut written) };
    check(rc, "drain")?;
    Ok(records
        .into_iter()
        .take(written as usize)
        .map(|row| KernelFired {
            handle: KernelHandle {
                index: row.handle_index,
                generation: row.handle_generation,
                context: row.handle_context,
            },
            due: row.due,
            schedule_sequence: row.schedule_sequence,
            dispatch_id: row.slot_dispatch_id,
        })
        .collect())
}

fn check(status_code: i32, op: &str) -> Result<(), KernelError> {
    if status_code == 0 {
        Ok(())
    } else {
        Err(status(status_code, op))
    }
}

fn status(status: i32, op: &str) -> KernelError {
    KernelError {
        status,
        detail: op.to_owned(),
    }
}

/// Discovers the native DLL via `LUMIO_ENGINE_NATIVE`. Missing is BLOCKED.
///
/// # Errors
///
/// Returns BLOCKED when the env var is unset or the file is missing.
pub fn engine_native_from_env() -> Result<PathBuf, String> {
    let path = PathBuf::from(
        std::env::var("LUMIO_ENGINE_NATIVE")
            .map_err(|_| "BLOCKED: LUMIO_ENGINE_NATIVE is not set".to_owned())?,
    );
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "BLOCKED: LUMIO_ENGINE_NATIVE missing: {}",
            path.display()
        ))
    }
}
