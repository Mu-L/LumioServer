#!/usr/bin/env python3
"""Run real checks, recording commands, exit codes, scope, source and logs.

A successful rust profile is not Native/CoreCLR/Game/Platform integration.
An integration run requires a pinned local input manifest; missing inputs fail.
No test output, missing observation, or failed subprocess is replaced with PASS.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def git_sha(path: Path) -> str:
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


def validate_inputs(manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    repos = manifest["repositories"]
    required = {"LumioServer", "LumioGameEngine", "LumioGameRuntime", "LumioGame", "LumioPlatform"}
    if not required.issubset(repos):
        raise ValueError("input manifest must pin Server, Engine, Runtime, Game and Platform")
    for name, row in repos.items():
        path = Path(row["path"]).resolve()
        expected = row["sha"]
        if not re.fullmatch(r"[0-9a-f]{40}", expected) or git_sha(path) != expected:
            raise ValueError(f"repository SHA mismatch: {name}")
        if subprocess.check_output(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=no"], text=True).strip():
            raise ValueError(f"repository has modified tracked inputs: {name}")
        if name == "LumioServer" and path != ROOT:
            raise ValueError("Server input must be the checkout running this verifier")
    artifacts = manifest.get("artifacts", {})
    if not artifacts:
        raise ValueError("input manifest must identify the actual SDK/managed artifacts")
    for name, row in artifacts.items():
        path = Path(row["path"])
        digest = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
        if digest != row["sha256"]:
            raise ValueError(f"artifact SHA mismatch: {name}")
    game = Path(repos["LumioGame"]["path"]).resolve()
    if Path(os.environ.get("LUMIO_GAME_ROOT", "")).resolve() != game:
        raise ValueError("LUMIO_GAME_ROOT must match the pinned Game input")
    return manifest


def commands(profile: str) -> list[list[str]]:
    if profile == "rust":
        return [
            ["node", ".spec/tools/spec-lint.mjs"],
            ["node", "--test", ".spec/tools/spec-lint.test.mjs"],
            ["cargo", "fmt", "--all", "--", "--check"],
            ["cargo", "check", "--workspace", "--locked"],
            ["cargo", "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"],
            ["cargo", "test", "-p", "lumio-host-runtime", "--locked"],
            ["cargo", "test", "-p", "lumio-host-testkit", "--locked"],
            ["cargo", "test", "-p", "lumio-server-process", "--all-features", "--locked", "--lib", "--bins",
             "--test", "entity_chat_architecture", "--test", "entity_chat_wire", "--test", "native_loader_architecture",
             "--test", "entity_chat_host", "--test", "secure_transport"],
        ]
    if profile == "managed":
        host = "entity-chat-host/src/Lumio.Server.EntityChat.HostEntry/Lumio.Server.EntityChat.HostEntry.csproj"
        account = "account-server/build.proj"
        tests = "account-server/tests/Lumio.Server.Account.Tests/Lumio.Server.Account.Tests.csproj"
        return [["dotnet", "restore", host, "--locked-mode"],
                ["dotnet", "build", host, "--no-restore", "-c", "Release"],
                ["dotnet", "restore", account, "--locked-mode"],
                ["dotnet", "build", account, "--no-restore", "-c", "Release"],
                ["dotnet", "test", tests, "--no-restore", "-c", "Release"]]
    return [["cargo", "test", "-p", "lumio-server-process", "--all-features", "--locked", "--test", "entity_chat_acceptance", "--", "--nocapture"]]


def run(profile: str, out: Path, inputs: Path | None) -> int:
    out.mkdir(parents=True, exist_ok=True)
    report = {"profile": profile, "sourceSha": git_sha(ROOT), "status": "RUNNING", "checks": []}
    result_path = out / f"{profile}-result.json"
    try:
        if profile == "integration":
            if inputs is None:
                raise ValueError("integration requires --inputs with pinned repositories and actual artifact hashes")
            report["resolvedInputs"] = validate_inputs(inputs)
        for index, cmd in enumerate(commands(profile)):
            name = f"{profile}-{index:02d}"
            started = time.monotonic()
            if shutil.which(cmd[0]) is None:
                raise ValueError(f"required tool unavailable: {cmd[0]}")
            try:
                completed = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           text=True, encoding="utf-8", errors="replace", timeout=600, check=False)
                text, code = completed.stdout, completed.returncode
            except subprocess.TimeoutExpired as error:
                raw = error.stdout or b""
                text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
                text += "\nVERIFIER_DEADLINE_EXCEEDED\n"
                code = 124
            (out / f"{name}.log").write_text(text, encoding="utf-8")
            row = {"command": cmd, "exitCode": code, "seconds": round(time.monotonic() - started, 3), "log": f"{name}.log"}
            report["checks"].append(row)
            print(f"CHECK {name}: {'PASS' if code == 0 else 'FAIL'}: {' '.join(cmd)}", flush=True)
            lines = text.splitlines()
            if code:
                selected = set(range(max(0, len(lines) - 70), len(lines)))
                for i, line in enumerate(lines):
                    if re.search(r"(^error|error [A-Z]+\d+|FAILED|panicked at|fatal error)", line):
                        selected.update(range(i, min(i + 18, len(lines))))
                print("\n".join(lines[i] for i in sorted(selected)), flush=True)
            else:
                print("\n".join(line for line in lines if "test result:" in line or "Passed!" in line), flush=True)
        report["status"] = "PASS" if all(row["exitCode"] == 0 for row in report["checks"]) else "FAIL"
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        report["status"] = "BLOCKED_ENV"
        report["error"] = str(error)
        print(f"BLOCKED_ENV: {error}", flush=True)
    finally:
        result_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"RESULT source={report['sourceSha']} profile={profile} status={report['status']}", flush=True)
    return 0 if report["status"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("rust", "managed", "integration"), required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts" / "verification")
    parser.add_argument("--inputs", type=Path)
    args = parser.parse_args()
    return run(args.profile, args.out.resolve(), args.inputs)


if __name__ == "__main__":
    sys.exit(main())
