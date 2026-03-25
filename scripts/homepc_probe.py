#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
from typing import Any


DEFAULT_HOST = os.getenv("FINAGENT_HOMEPC_HOST", "yuanhaizhou@192.168.1.17")
DEFAULT_TIMEOUT = int(os.getenv("FINAGENT_HOMEPC_SSH_TIMEOUT_SECONDS", "12"))


SSH_BASE = [
    "timeout",
    str(DEFAULT_TIMEOUT),
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    f"ConnectTimeout={DEFAULT_TIMEOUT}",
    DEFAULT_HOST,
]


def run_ssh(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        SSH_BASE + [command],
        text=True,
        capture_output=True,
        check=False,
    )


def parse_lines(text: str) -> list[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def main() -> int:
    payload: dict[str, Any] = {
        "host": DEFAULT_HOST,
        "timeout_seconds": DEFAULT_TIMEOUT,
        "checks": {},
    }

    checks = {
        "identity": "hostname && uname -sr",
        "gpu": "nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu --format=csv,noheader",
        "gpu_processes": "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_memory --format=csv,noheader || true",
        "ollama_service": "systemctl is-active ollama || true",
        "ollama_models": "ollama list 2>/dev/null || true",
        "system_ffmpeg": "bash -lc 'command -v ffmpeg || true; command -v ffprobe || true; ffmpeg -version 2>/dev/null | sed -n \"1,2p\" || true'",
        "conda_envs": "ls -1 /home/yuanhaizhou/miniconda3/envs 2>/dev/null || true",
        "soulxpodcast_ffmpeg": (
            "bash -lc 'source \"$HOME/miniconda3/etc/profile.d/conda.sh\" && "
            "conda activate soulxpodcast && "
            "command -v ffmpeg && command -v ffprobe && "
            "ffmpeg -version 2>/dev/null | sed -n \"1,2p\"'"
        ),
        "default_python_modules": (
            "python3 - <<'PY'\n"
            "mods=[]\n"
            "for name in ['funasr','modelscope','torch']:\n"
            "    try:\n"
            "        __import__(name)\n"
            "        mods.append({'name': name, 'installed': True})\n"
            "    except Exception:\n"
            "        mods.append({'name': name, 'installed': False})\n"
            "print(__import__('json').dumps(mods, ensure_ascii=False))\n"
            "PY"
        ),
        "funasr_runtime_candidates": (
            "bash -lc 'for py in "
            "/home/yuanhaizhou/miniconda3/envs/*/bin/python "
            "/home/yuanhaizhou/*/.venv*/bin/python "
            "/home/yuanhaizhou/.venv*/bin/python; "
            "do [ -x \"$py\" ] || continue; "
            "out=$(\"$py\" - <<\"PY\" 2>/dev/null\n"
            "from importlib.util import find_spec\n"
            "mods=[]\n"
            "for name in [\"funasr\",\"modelscope\",\"torch\"]:\n"
            "    if find_spec(name):\n"
            "        mods.append(name)\n"
            "print(\",\".join(mods))\n"
            "PY\n"
            "); [ -n \"$out\" ] && echo \"$py => $out\"; done'"
        ),
        "funasr_assets": (
            "bash -lc 'for d in "
            "/home/yuanhaizhou/funasr_models "
            "/home/yuanhaizhou/.cache/modelscope/hub "
            "/home/yuanhaizhou/storyplay-tts; "
            "do [ -e \"$d\" ] || continue; "
            "echo ===DIR:$d===; "
            "find \"$d\" -maxdepth 4 \\( "
            "-iname \"*paraformer*\" -o "
            "-iname \"*fsmn*\" -o "
            "-iname \"*ct-transformer*\" -o "
            "-iname \"*punc*\" -o "
            "-iname \"*funasr*\" "
            "\\) 2>/dev/null | sort | sed -n \"1,200p\"; done'"
        ),
        "paths": (
            "for p in "
            "/home/yuanhaizhou/funasr_models "
            "/home/yuanhaizhou/.cache/modelscope/hub "
            "/home/yuanhaizhou/storyplay-tts/.venv-metal/bin/funasr "
            "/home/yuanhaizhou/miniconda3/envs/soulxpodcast/bin/python "
            "/home/yuanhaizhou/miniconda3/envs/GPTSoVits/bin/python "
            "/home/yuanhaizhou/maint "
            "; "
            "do [ -e \"$p\" ] && echo EXISTS:$p || echo MISS:$p; done"
        ),
    }

    for name, cmd in checks.items():
        proc = run_ssh(cmd)
        payload["checks"][name] = {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout_lines": parse_lines(proc.stdout),
            "stderr_lines": parse_lines(proc.stderr),
            "command": cmd if not name.endswith("modules") else f"{name} inline probe",
        }

    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
