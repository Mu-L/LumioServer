//! R-00484 (Owner ruling D24): the workspace keeps exactly one cross-platform
//! native loader. No Windows-only import library, no second `RootApiV1`, no
//! platform gates standing in for a portable implementation.
//!
//! The banned symbols are spelled with `concat!` so this file does not itself
//! trip the token scans in `entity_chat_architecture.rs` — same idiom as the
//! `banned_*_token` helpers there.

use std::fs;
use std::path::{Path, PathBuf};

fn modules_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("modules/")
        .to_path_buf()
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Every `modules/*/src/**/*.rs` file, excluding this test itself.
fn production_sources() -> Vec<(PathBuf, String)> {
    let mut files = Vec::new();
    for crate_dir in ["host-runtime", "process"] {
        collect_rust_sources(&modules_root().join(crate_dir).join("src"), &mut files);
    }
    files
        .into_iter()
        .filter_map(|path| fs::read_to_string(&path).ok().map(|text| (path, text)))
        .collect()
}

/// Windows-only dynamic-loading symbols, split so the literals never appear
/// whole in this file.
fn banned_loader_symbols() -> [&'static str; 4] {
    [
        concat!("kernel", "32"),
        concat!("LoadLibrary", "W"),
        concat!("GetProc", "Address"),
        concat!("Free", "Library"),
    ]
}

#[test]
fn no_windows_only_dynamic_loading_symbols_remain() {
    let mut offenders = Vec::new();
    for (path, text) in production_sources() {
        for symbol in banned_loader_symbols() {
            if text.contains(symbol) {
                offenders.push(format!("{}: {symbol}", path.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "D24: dynamic loading must go through the single portable loader \
         (libloading); found Windows-only symbols in:\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn root_api_table_is_defined_exactly_once() {
    let definitions: Vec<PathBuf> = production_sources()
        .into_iter()
        .filter(|(_, text)| text.contains("struct RootApiV1"))
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        definitions.len(),
        1,
        "D24: RootApiV1 must have a single definition; found {definitions:?}"
    );
    let path = definitions[0].to_string_lossy().replace('\\', "/");
    assert!(
        path.contains("modules/host-runtime/src/"),
        "the single root table belongs to lumio-host-runtime, found at {path}"
    );
}

#[test]
fn the_native_loader_carries_no_platform_gates() {
    for (path, text) in production_sources() {
        let name = path.to_string_lossy().replace('\\', "/");
        if !name.contains("native_") && !name.contains("sdk_loader") {
            continue;
        }
        for banned in [
            concat!("cfg(", "windows)"),
            concat!("cfg(not(", "windows))"),
            concat!("allow(dead_", "code)"),
        ] {
            assert!(
                !text.contains(banned),
                "D24 forbids `{banned}` in {name}: the loader must be portable, \
                 not gated"
            );
        }
    }
}
