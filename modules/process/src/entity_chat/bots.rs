//! Discover and spawn `Lumio.Client.Bot.Host`. Evidence is its log directory.

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const FLEET_WAIT: Duration = Duration::from_secs(15);
const FLEET_PROGRESS_POLL: Duration = Duration::from_millis(1);
const R4_04_BLOCKED: &str = "BLOCKED: 等 R4-04";
const BOT_CHAT_CADENCE_TICKS: [u64; 3] = [5, 10, 15];

/// Observed Bot.Host log evidence. Empty unless R4-04 Bot.Host wrote logs.
#[derive(Debug, Clone, Default)]
pub struct ClientBotTrace {
    pub tick_source: String,
    pub utterance_ticks: Vec<u64>,
    pub timer_manager_invoked: bool,
    pub submitted: u32,
    pub pid: u32,
    pub input: Option<ClientInputEvidence>,
    pub blocked: Option<String>,
}

/// Input envelope metadata emitted by the Client Bot after Runtime encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInputEvidence {
    pub message_type: String,
    pub mapping_id: String,
    pub payload_sha256: String,
}

impl ClientBotTrace {
    /// Returns the Runtime-encoded input metadata exactly as emitted by Client.
    #[must_use]
    pub fn input_evidence_json(&self) -> Option<Value> {
        self.input.as_ref().map(|input| {
            json!({
                "messageType": input.message_type,
                "mappingId": input.mapping_id,
                "payloadSha256": input.payload_sha256,
            })
        })
    }
}

/// Live Bot.Host process until [`ClientBotFleet::release`].
pub struct ClientBotFleet {
    pub trace: ClientBotTrace,
    child: Option<Child>,
    release_path: PathBuf,
    log_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    expected_submissions: u32,
}

impl ClientBotFleet {
    /// Signals Bot.Host to stop after Room observed chat.event.
    pub fn release(mut self) {
        self.release_mut();
    }

    fn release_mut(&mut self) {
        let _ = std::fs::write(&self.release_path, "release\n");
        let Some(mut child) = self.child.take() else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match child.try_wait() {
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
                Ok(Some(_)) | Err(_) => break,
            }
        }
    }
}

impl Drop for ClientBotFleet {
    fn drop(&mut self) {
        self.release_mut();
    }
}

/// Env lookup used by discovery. Process env in production; map in unit tests.
pub trait BotHostEnv {
    /// Reads one environment variable.
    ///
    /// # Errors
    ///
    /// Returns [`std::env::VarError`] when the name is unset or invalid.
    fn var(&self, name: &str) -> Result<String, std::env::VarError>;
}

struct StdEnv;

impl BotHostEnv for StdEnv {
    fn var(&self, name: &str) -> Result<String, std::env::VarError> {
        std::env::var(name)
    }
}

struct BotHostLaunch {
    server: String,
    account_from: String,
    account_to: String,
    engine_native: PathBuf,
    log_dir: PathBuf,
}

/// Locates `Lumio.Client.Bot.Host` via `LumioClientRoot` / `LUMIO_CLIENT_ROOT` /
/// `LUMIO_BOT_HOST` or a `LumioClient` sibling of this repo. Missing is BLOCKED.
///
/// # Errors
///
/// Returns a BLOCKED reason when no host dll/exe/csproj can be found.
pub fn discover_bot_host() -> Result<PathBuf, String> {
    discover_bot_host_in(&StdEnv, &process_repo_root())
}

pub(crate) fn discover_bot_host_in(env: &dyn BotHostEnv, repo: &Path) -> Result<PathBuf, String> {
    if let Some(raw) = env_first(env, &["LUMIO_BOT_HOST"]) {
        let path = PathBuf::from(raw);
        if path.is_file() {
            return Ok(path);
        }
        if path.is_dir() {
            if let Some(found) = bot_host_in_dir(&path) {
                return Ok(found);
            }
        }
        return Err(format!(
            "BLOCKED: LUMIO_BOT_HOST missing: {}",
            path.display()
        ));
    }

    let mut roots = Vec::new();
    if let Some(root) = env_first(env, &["LumioClientRoot", "LUMIO_CLIENT_ROOT"]) {
        roots.push(PathBuf::from(root));
    }
    if let Some(parent) = repo.parent() {
        roots.push(parent.join("LumioClient"));
        if let Some(grand) = parent.parent() {
            roots.push(grand.join("LumioClient"));
        }
    }
    for root in roots {
        if !root.is_dir() {
            continue;
        }
        if let Some(found) = bot_host_under_client(&root) {
            return Ok(found);
        }
        let csproj = root.join("modules/bot/host/Lumio.Client.Bot.Host.csproj");
        if csproj.is_file() {
            return Ok(csproj);
        }
    }
    Err(
        "BLOCKED: Lumio.Client.Bot.Host not found (set LumioClientRoot, LUMIO_CLIENT_ROOT, or LUMIO_BOT_HOST)"
            .to_owned(),
    )
}

/// Builds Bot.Host when discovery returned a csproj; otherwise returns the file.
///
/// # Errors
///
/// Returns BLOCKED when `dotnet build` fails or the output dll is missing.
pub fn ensure_bot_host_executable(path: &Path, dotnet: &str) -> Result<PathBuf, String> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("csproj") {
        return build_bot_host(path, dotnet);
    }
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    Err(format!(
        "BLOCKED: Lumio.Client.Bot.Host missing: {}",
        path.display()
    ))
}

/// Spawns `Lumio.Client.Bot.Host` and reads its log directory. No injection.
///
/// # Errors
///
/// Returns BLOCKED when the host is missing, or when logs are absent (R4-04).
pub fn run_client_bot_fleet<F>(
    bot_host: &Path,
    engine_native: &Path,
    room_uri: &str,
    bot_count: u32,
    out_dir: &Path,
    dotnet: &str,
    on_progress: F,
) -> Result<ClientBotFleet, String>
where
    F: FnMut(),
{
    let fleet = start_client_bot_fleet(
        bot_host,
        engine_native,
        room_uri,
        bot_count,
        out_dir,
        dotnet,
    )?;
    wait_for_client_bot_fleet(fleet, on_progress)
}

/// Starts Bot.Host without waiting for sessions to become active.
///
/// # Errors
///
/// Returns BLOCKED when the host cannot be built or spawned.
pub fn start_client_bot_fleet(
    bot_host: &Path,
    engine_native: &Path,
    room_uri: &str,
    bot_count: u32,
    out_dir: &Path,
    dotnet: &str,
) -> Result<ClientBotFleet, String> {
    std::fs::create_dir_all(out_dir).map_err(|error| error.to_string())?;
    let host = ensure_bot_host_executable(bot_host, dotnet)?;
    let launch = bot_host_launch(room_uri, bot_count, engine_native, out_dir);
    let release_path = launch.log_dir.join("release.flag");
    let stdout_path = launch.log_dir.join("bot-host.stdout");
    let stderr_path = launch.log_dir.join("bot-host.stderr");
    let stdout = File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = File::create(&stderr_path).map_err(|error| error.to_string())?;
    let mut command = bot_host_command(dotnet, &host);
    apply_bot_host_launch(&mut command, &launch);
    command
        .env("DOTNET_NOLOGO", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let child = command
        .spawn()
        .map_err(|error| format!("BLOCKED: spawn Lumio.Client.Bot.Host: {error}"))?;
    Ok(ClientBotFleet {
        trace: ClientBotTrace::default(),
        child: Some(child),
        release_path,
        log_dir: launch.log_dir,
        stdout_path,
        stderr_path,
        expected_submissions: expected_submission_count(bot_count),
    })
}

/// Waits for Bot.Host evidence while the suite advances host work.
///
/// # Errors
///
/// Returns BLOCKED when Bot.Host exits or times out before evidence.
pub fn wait_for_client_bot_fleet<F>(
    mut fleet: ClientBotFleet,
    mut on_progress: F,
) -> Result<ClientBotFleet, String>
where
    F: FnMut(),
{
    let deadline = Instant::now() + FLEET_WAIT;
    loop {
        on_progress();
        match try_read_bot_host_logs(&fleet.log_dir) {
            Ok(Some(trace)) => {
                let complete = trace.submitted == fleet.expected_submissions
                    && trace.submitted == super::BOT_COUNT
                    && trace.utterance_ticks == BOT_CHAT_CADENCE_TICKS;
                fleet.trace = trace;
                if complete {
                    return Ok(fleet);
                }
            }
            Ok(None) => {}
            Err(reason) => return Err(reason),
        }
        let Some(child) = fleet.child.as_mut() else {
            return Err("BLOCKED: Lumio.Client.Bot.Host process missing".to_owned());
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!(
                    "{R4_04_BLOCKED}: Lumio.Client.Bot.Host exited {status} without log evidence{}",
                    tail_logs(&fleet.stdout_path, &fleet.stderr_path)
                ));
            }
            Ok(None) => {}
            Err(error) => {
                return Err(format!("BLOCKED: Lumio.Client.Bot.Host wait: {error}"));
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "{R4_04_BLOCKED}: Lumio.Client.Bot.Host timed out without log evidence{}",
                tail_logs(&fleet.stdout_path, &fleet.stderr_path)
            ));
        }
        thread::sleep(FLEET_PROGRESS_POLL);
    }
}

fn process_repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn env_first(env: &dyn BotHostEnv, names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(value) = env.var(name) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn bot_host_under_client(root: &Path) -> Option<PathBuf> {
    bot_host_in_dir(&root.join("modules/bot/host/bin/Debug/net10.0"))
        .or_else(|| bot_host_in_dir(&root.join("modules/bot/host/bin/Release/net10.0")))
}

fn bot_host_in_dir(dir: &Path) -> Option<PathBuf> {
    first_existing(&[
        dir.join("Lumio.Client.Bot.Host.dll"),
        dir.join("Lumio.Client.Bot.Host.exe"),
    ])
}

fn first_existing(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates.iter().find(|path| path.is_file()).cloned()
}

fn build_bot_host(csproj: &Path, dotnet: &str) -> Result<PathBuf, String> {
    let status = Command::new(dotnet)
        .arg("build")
        .arg(csproj)
        .arg("-c")
        .arg("Debug")
        .arg("--nologo")
        .status()
        .map_err(|error| format!("BLOCKED: dotnet build Lumio.Client.Bot.Host: {error}"))?;
    if !status.success() {
        return Err(format!(
            "BLOCKED: dotnet build Lumio.Client.Bot.Host failed: {status}"
        ));
    }
    let dir = csproj.parent().unwrap_or(csproj);
    bot_host_in_dir(&dir.join("bin/Debug/net10.0"))
        .or_else(|| bot_host_in_dir(&dir.join("bin/Release/net10.0")))
        .ok_or_else(|| "BLOCKED: Lumio.Client.Bot.Host.dll missing after dotnet build".to_owned())
}

fn bot_host_launch(
    server: &str,
    bot_count: u32,
    engine_native: &Path,
    log_dir: &Path,
) -> BotHostLaunch {
    let count = bot_count.max(1);
    BotHostLaunch {
        server: server.to_owned(),
        account_from: super::bot_name(1),
        account_to: super::bot_name(count),
        engine_native: engine_native.to_path_buf(),
        log_dir: log_dir.to_path_buf(),
    }
}

const fn expected_submission_count(bot_count: u32) -> u32 {
    bot_count
}

fn apply_bot_host_launch(command: &mut Command, launch: &BotHostLaunch) {
    command
        .arg("--server")
        .arg(&launch.server)
        .arg("--account-from")
        .arg(&launch.account_from)
        .arg("--account-to")
        .arg(&launch.account_to)
        .arg("--engine-native")
        .arg(&launch.engine_native)
        .arg("--log-dir")
        .arg(&launch.log_dir)
        .env("LumioBotServer", &launch.server)
        .env("LumioBotAccountFrom", &launch.account_from)
        .env("LumioBotAccountTo", &launch.account_to)
        .env("LumioEngineNative", &launch.engine_native)
        .env("LUMIO_ENGINE_NATIVE", &launch.engine_native)
        .env("LumioBotLogDir", &launch.log_dir);
}

fn bot_host_command(dotnet: &str, host: &Path) -> Command {
    let ext = host.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("dll") {
        let mut command = Command::new(dotnet);
        command.arg("exec").arg(host);
        command
    } else {
        Command::new(host)
    }
}

fn tail_logs(stdout_path: &Path, stderr_path: &Path) -> String {
    let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(stderr_path).unwrap_or_default();
    let mut logs = String::new();
    if !stdout.trim().is_empty() {
        logs.push_str(" stdout=");
        logs.push_str(stdout.trim());
    }
    if !stderr.trim().is_empty() {
        logs.push_str(" stderr=");
        logs.push_str(stderr.trim());
    }
    logs
}

fn is_bot_host_log_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    if name.eq_ignore_ascii_case("timer-trace.json")
        || name.eq_ignore_ascii_case("fleet-spec.json")
        || name.eq_ignore_ascii_case("release.flag")
    {
        return false;
    }
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(ext.as_str(), "ndjson" | "jsonl" | "log") || name == "bot-host.stdout"
}

fn try_read_bot_host_logs(log_dir: &Path) -> Result<Option<ClientBotTrace>, String> {
    let mut submitted = 0_u32;
    let mut utterance_ticks = Vec::new();
    let mut submitted_accounts = HashSet::with_capacity(super::BOT_COUNT as usize);
    let expected_accounts: HashSet<String> = (1..=super::BOT_COUNT).map(super::bot_name).collect();
    let mut tick_source = String::new();
    let mut pid = 0_u32;
    let mut input = None;
    let entries = match std::fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(_) => {
            return Err(format!(
                "{R4_04_BLOCKED}: Lumio.Client.Bot.Host logs missing"
            ));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!("{R4_04_BLOCKED}: cannot read Bot.Host log directory entry: {error}")
        })?;
        let path = entry.path();
        if !path.is_file() || !is_bot_host_log_file(&path) {
            continue;
        }
        let text = std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "{R4_04_BLOCKED}: cannot read Bot.Host log {}: {error}",
                path.display()
            )
        })?;
        for (line_index, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_str::<Value>(trimmed).map_err(|error| {
                format!(
                    "{R4_04_BLOCKED}: malformed JSON in {} line {}: {error}",
                    path.display(),
                    line_index + 1
                )
            })?;
            if !value.is_object() {
                return Err(format!(
                    "{R4_04_BLOCKED}: Bot.Host log {} line {} must be a JSON object",
                    path.display(),
                    line_index + 1
                ));
            }
            if let Some(source) = value.get("tickSource").and_then(Value::as_str) {
                if tick_source.is_empty() || source == "native-kernel/tickFrame" {
                    source.clone_into(&mut tick_source);
                }
            }
            if let Some(process_id) = value.get("pid").and_then(Value::as_u64) {
                pid = u32::try_from(process_id).unwrap_or(pid);
            }
            if value.get("kind").and_then(Value::as_str) != Some("chat.input") {
                continue;
            }
            let evidence = parse_input_evidence(&value).ok_or_else(|| {
                format!(
                    "{R4_04_BLOCKED}: malformed Runtime InputCommand metadata in Client Bot.Host logs"
                )
            })?;
            if input.is_none() {
                input = Some(evidence);
            }
            let account_id = value
                .get("accountId")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    format!(
                        "{R4_04_BLOCKED}: chat.input missing accountId in {} line {}",
                        path.display(),
                        line_index + 1
                    )
                })?;
            if !expected_accounts.contains(account_id) {
                return Err(format!(
                    "{R4_04_BLOCKED}: unexpected Bot accountId {account_id} in {} line {}",
                    path.display(),
                    line_index + 1
                ));
            }
            if !submitted_accounts.insert(account_id.to_owned()) {
                return Err(format!(
                    "{R4_04_BLOCKED}: duplicate Bot accountId {account_id} in {} line {}",
                    path.display(),
                    line_index + 1
                ));
            }
            let tick = value.get("tick").and_then(Value::as_u64).ok_or_else(|| {
                format!(
                    "{R4_04_BLOCKED}: chat.input missing tick in {} line {}",
                    path.display(),
                    line_index + 1
                )
            })?;
            if !BOT_CHAT_CADENCE_TICKS.contains(&tick) {
                return Err(format!(
                    "{R4_04_BLOCKED}: unexpected chat.input tick {tick} in {} line {}",
                    path.display(),
                    line_index + 1
                ));
            }
            submitted += 1;
            utterance_ticks.push(tick);
        }
    }
    if submitted == 0 {
        return Ok(None);
    }
    if input.is_none() {
        return Err(format!(
            "{R4_04_BLOCKED}: Lumio.Client.Bot.Host logs missing Runtime InputCommand metadata"
        ));
    }
    utterance_ticks.sort_unstable();
    utterance_ticks.dedup();
    if submitted == super::BOT_COUNT
        && (submitted_accounts != expected_accounts
            || utterance_ticks.as_slice() != BOT_CHAT_CADENCE_TICKS)
    {
        return Err(format!(
            "{R4_04_BLOCKED}: complete Bot fleet must contain Bot01 through Bot100 across ticks 5, 10, and 15"
        ));
    }
    Ok(Some(ClientBotTrace {
        timer_manager_invoked: tick_source == "native-kernel/tickFrame"
            && !utterance_ticks.is_empty(),
        tick_source,
        utterance_ticks,
        submitted,
        pid,
        input,
        blocked: None,
    }))
}

fn parse_input_evidence(value: &Value) -> Option<ClientInputEvidence> {
    let message_type = value.get("messageType")?.as_str()?;
    let mapping_id = value.get("mappingId")?.as_str()?;
    let payload_sha256 = value.get("payloadSha256")?.as_str()?;
    if message_type != "InputCommand"
        || mapping_id != "chat.input"
        || !is_lower_sha256(payload_sha256)
    {
        return None;
    }
    Some(ClientInputEvidence {
        message_type: message_type.to_owned(),
        mapping_id: mapping_id.to_owned(),
        payload_sha256: payload_sha256.to_owned(),
    })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::{
        bot_host_launch, discover_bot_host_in, expected_submission_count, try_read_bot_host_logs,
        wait_for_client_bot_fleet, BotHostEnv, ClientBotFleet, ClientBotTrace, R4_04_BLOCKED,
    };
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    struct MapEnv(HashMap<String, String>);

    const INPUT_SHA256: &str = "5dbd584f1718b8bcd0dab4abeea83169f4a990defab81a8316ed845798d92dab";

    impl BotHostEnv for MapEnv {
        fn var(&self, name: &str) -> Result<String, std::env::VarError> {
            self.0
                .get(name)
                .cloned()
                .ok_or(std::env::VarError::NotPresent)
        }
    }

    fn chat_input_line(account_id: &str, tick: u64) -> String {
        serde_json::json!({
            "kind": "chat.input",
            "tickSource": "native-kernel/tickFrame",
            "tick": tick,
            "messageType": "InputCommand",
            "mappingId": "chat.input",
            "payloadSha256": INPUT_SHA256,
            "accountId": account_id,
        })
        .to_string()
    }

    fn bot_fleet_lines(count: u32) -> Vec<String> {
        (1..=count)
            .map(|index| {
                let tick = match (index - 1) % 3 {
                    0 => 5,
                    1 => 10,
                    _ => 15,
                };
                chat_input_line(&format!("Bot{index:02}"), tick)
            })
            .collect()
    }

    fn write_bot_fleet_log(log_dir: &Path, lines: &[String]) {
        let mut text = lines.join("\n");
        text.push('\n');
        fs::write(log_dir.join("bot-host.ndjson"), text).expect("ndjson");
    }

    fn fleet_for_log_dir(log_dir: &Path) -> ClientBotFleet {
        ClientBotFleet {
            trace: ClientBotTrace::default(),
            child: None,
            release_path: log_dir.join("release.flag"),
            log_dir: log_dir.to_path_buf(),
            stdout_path: log_dir.join("stdout"),
            stderr_path: log_dir.join("stderr"),
            expected_submissions: 100,
        }
    }

    #[test]
    fn missing_client_bot_host_is_blocked() {
        let repo = tempfile::tempdir().expect("tmp");
        let err = discover_bot_host_in(&MapEnv(HashMap::new()), repo.path()).unwrap_err();
        assert!(err.starts_with("BLOCKED:"), "{err}");
        assert!(
            err.contains("LumioClientRoot")
                || err.contains("LUMIO_CLIENT_ROOT")
                || err.contains("LUMIO_BOT_HOST"),
            "{err}"
        );
    }

    #[test]
    fn lumio_bot_host_file_is_discovered() {
        let tmp = tempfile::tempdir().expect("tmp");
        let host = tmp.path().join("Lumio.Client.Bot.Host.dll");
        fs::write(&host, []).expect("touch");
        let mut env = HashMap::new();
        env.insert(
            "LUMIO_BOT_HOST".to_owned(),
            host.to_string_lossy().into_owned(),
        );
        let found = discover_bot_host_in(&MapEnv(env), tmp.path()).expect("discover");
        assert_eq!(found, host);
    }

    #[test]
    fn lumio_client_root_csproj_is_discovered() {
        let tmp = tempfile::tempdir().expect("tmp");
        let csproj = tmp
            .path()
            .join("modules/bot/host/Lumio.Client.Bot.Host.csproj");
        fs::create_dir_all(csproj.parent().expect("dir")).expect("dirs");
        fs::write(&csproj, "<Project />").expect("csproj");
        let mut env = HashMap::new();
        env.insert(
            "LUMIO_CLIENT_ROOT".to_owned(),
            tmp.path().to_string_lossy().into_owned(),
        );
        let found = discover_bot_host_in(&MapEnv(env), tmp.path()).expect("discover");
        assert_eq!(found, csproj);
    }

    #[test]
    fn lumio_client_root_pascal_is_discovered() {
        let tmp = tempfile::tempdir().expect("tmp");
        let csproj = tmp
            .path()
            .join("modules/bot/host/Lumio.Client.Bot.Host.csproj");
        fs::create_dir_all(csproj.parent().expect("dir")).expect("dirs");
        fs::write(&csproj, "<Project />").expect("csproj");
        let mut env = HashMap::new();
        env.insert(
            "LumioClientRoot".to_owned(),
            tmp.path().to_string_lossy().into_owned(),
        );
        let found = discover_bot_host_in(&MapEnv(env), tmp.path()).expect("discover");
        assert_eq!(found, csproj);
    }

    #[test]
    fn launch_spec_uses_inclusive_bot_account_range() {
        let spec = bot_host_launch(
            "ws://127.0.0.1:1/",
            100,
            Path::new("engine"),
            Path::new("logs"),
        );
        assert_eq!(spec.server, "ws://127.0.0.1:1/");
        assert_eq!(spec.account_from, "Bot01");
        assert_eq!(spec.account_to, "Bot100");
    }

    #[test]
    fn fleet_waits_for_exactly_one_submission_per_bot() {
        assert_eq!(expected_submission_count(100), 100);
    }

    #[test]
    fn fleet_rejects_99_submissions() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(99));
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("99 submissions must be incomplete");
        assert!(error.starts_with("BLOCKED:"), "{error}");
    }

    #[test]
    fn fleet_accepts_exactly_100_unique_bot_submissions() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(100));
        let fleet = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .expect("100 unique submissions");
        assert_eq!(fleet.trace.submitted, 100);
        assert_eq!(fleet.trace.utterance_ticks, [5, 10, 15]);
        assert!(fleet.trace.input.is_some());
    }

    #[test]
    fn fleet_rejects_101_submissions() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(101));
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("101 submissions must not satisfy exact evidence");
        assert!(error.contains("unexpected Bot accountId"), "{error}");
    }

    #[test]
    fn fleet_rejects_duplicate_and_missing_bot_accounts() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut lines = bot_fleet_lines(100);
        lines[99] = chat_input_line("Bot99", 5);
        write_bot_fleet_log(tmp.path(), &lines);
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("duplicate Bot99 and missing Bot100 must fail");
        assert!(error.contains("duplicate Bot accountId"), "{error}");
    }

    #[test]
    fn fleet_rejects_submissions_outside_cadence_ticks() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut lines = bot_fleet_lines(100);
        lines[0] = chat_input_line("Bot01", 20);
        write_bot_fleet_log(tmp.path(), &lines);
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("tick 20 must fail");
        assert!(error.contains("unexpected chat.input tick"), "{error}");
    }

    #[test]
    fn complete_fleet_requires_all_three_cadence_ticks() {
        let tmp = tempfile::tempdir().expect("tmp");
        let lines: Vec<String> = (1..=100)
            .map(|index| {
                let tick = if index % 2 == 0 { 10 } else { 5 };
                chat_input_line(&format!("Bot{index:02}"), tick)
            })
            .collect();
        write_bot_fleet_log(tmp.path(), &lines);
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("tick 15 evidence is required");
        assert!(error.contains("ticks 5, 10, and 15"), "{error}");
    }

    #[test]
    fn malformed_json_in_candidate_log_fails_closed() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(100));
        fs::write(tmp.path().join("extra.ndjson"), "not json\n").expect("malformed log");
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("malformed JSON must fail");
        assert!(error.contains("malformed"), "{error}");
    }

    #[test]
    fn non_object_json_in_candidate_log_fails_closed() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(100));
        fs::write(tmp.path().join("extra.jsonl"), "[]\n").expect("non-object log");
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("JSON line must be an object");
        assert!(error.contains("object"), "{error}");
    }

    #[test]
    fn truncated_json_line_fails_closed() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut text = bot_fleet_lines(100).join("\n");
        text.push_str("\n{\"kind\":\"chat.input\"");
        fs::write(tmp.path().join("bot-host.ndjson"), text).expect("truncated log");
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("truncated JSON must fail");
        assert!(error.contains("malformed"), "{error}");
    }

    #[test]
    fn unreadable_candidate_log_fails_closed() {
        let tmp = tempfile::tempdir().expect("tmp");
        write_bot_fleet_log(tmp.path(), &bot_fleet_lines(100));
        fs::write(tmp.path().join("unreadable.log"), [0xff, 0xfe]).expect("non-UTF-8 log");
        let error = wait_for_client_bot_fleet(fleet_for_log_dir(tmp.path()), || {})
            .err()
            .expect("unreadable log must fail");
        assert!(error.contains("read"), "{error}");
    }

    #[test]
    fn empty_log_dir_has_no_bot_host_evidence() {
        let tmp = tempfile::tempdir().expect("tmp");
        let trace = try_read_bot_host_logs(tmp.path()).expect("read empty log directory");
        assert!(trace.is_none());
    }

    #[test]
    fn timer_trace_json_is_not_bot_host_log_evidence() {
        let tmp = tempfile::tempdir().expect("tmp");
        fs::write(
            tmp.path().join("timer-trace.json"),
            r#"{"kind":"chat.input","tickSource":"native-kernel/tickFrame","tick":5}"#,
        )
        .expect("trace");
        let trace = try_read_bot_host_logs(tmp.path()).expect("read log directory");
        assert!(trace.is_none());
    }

    #[test]
    fn bot_host_ndjson_chat_input_is_log_evidence() {
        let tmp = tempfile::tempdir().expect("tmp");
        fs::write(
            tmp.path().join("bot-host.ndjson"),
            concat!(
                "{\"kind\":\"chat.input\",\"tickSource\":\"native-kernel/tickFrame\",",
                "\"tick\":5,\"messageType\":\"InputCommand\",\"mappingId\":\"chat.input\",",
                "\"payloadSha256\":\"5dbd584f1718b8bcd0dab4abeea83169f4a990defab81a8316ed845798d92dab\",",
                "\"accountId\":\"Bot01\"}\n"
            ),
        )
        .expect("ndjson");
        let trace = try_read_bot_host_logs(tmp.path())
            .expect("logs")
            .expect("chat.input trace");
        assert_eq!(trace.tick_source, "native-kernel/tickFrame");
        assert!(trace.utterance_ticks.contains(&5));
        assert_eq!(trace.submitted, 1);
        assert!(trace.timer_manager_invoked);
        assert!(trace.blocked.is_none());
    }

    #[test]
    fn bot_host_ndjson_preserves_runtime_input_evidence() {
        let tmp = tempfile::tempdir().expect("tmp");
        fs::write(
            tmp.path().join("bot-host.ndjson"),
            concat!(
                "{\"kind\":\"chat.input\",\"tickSource\":\"native-kernel/tickFrame\",",
                "\"tick\":5,\"messageType\":\"InputCommand\",\"mappingId\":\"chat.input\",",
                "\"payloadSha256\":\"5dbd584f1718b8bcd0dab4abeea83169f4a990defab81a8316ed845798d92dab\",",
                "\"accountId\":\"Bot01\"}\n"
            ),
        )
        .expect("ndjson");
        let trace = try_read_bot_host_logs(tmp.path())
            .expect("logs")
            .expect("chat.input trace");
        let input = trace.input.expect("input evidence");
        assert_eq!(input.message_type, "InputCommand");
        assert_eq!(input.mapping_id, "chat.input");
        assert_eq!(input.payload_sha256.len(), 64);
    }

    #[test]
    fn bot_host_chat_input_without_runtime_wire_metadata_is_blocked() {
        let tmp = tempfile::tempdir().expect("tmp");
        fs::write(
            tmp.path().join("bot-host.ndjson"),
            "{\"kind\":\"chat.input\",\"tickSource\":\"native-kernel/tickFrame\",\"tick\":5}\n",
        )
        .expect("ndjson");
        let err = try_read_bot_host_logs(tmp.path()).expect_err("metadata is required");
        assert!(err.starts_with(R4_04_BLOCKED), "{err}");
    }

    #[test]
    fn fleet_release_writes_release_path() {
        let tmp = tempfile::tempdir().expect("tmp");
        let release_path = tmp.path().join("release.flag");
        let fleet = ClientBotFleet {
            trace: ClientBotTrace::default(),
            child: None,
            release_path: release_path.clone(),
            log_dir: tmp.path().to_path_buf(),
            stdout_path: tmp.path().join("stdout"),
            stderr_path: tmp.path().join("stderr"),
            expected_submissions: 0,
        };
        fleet.release();
        assert!(
            release_path.is_file(),
            "suite release must create the Bot.Host stop file"
        );
    }
}
