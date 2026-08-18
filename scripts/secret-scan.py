#!/usr/bin/env python3
"""Fail CI if tracked text files contain high-confidence credential patterns."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

PATTERNS = (
    ("OpenAI-style API key", re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b")),
    ("GitHub token", re.compile(rb"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    ("private key", re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
)
MAX_FILE_BYTES = 2 * 1024 * 1024


def tracked_files() -> list[pathlib.Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        stdout=subprocess.PIPE,
    )
    return [pathlib.Path(entry) for entry in result.stdout.decode().split("\0") if entry]


def main() -> int:
    findings: list[str] = []
    for path in tracked_files():
        try:
            data = path.read_bytes()
        except (OSError, IsADirectoryError):
            continue
        if len(data) > MAX_FILE_BYTES or b"\0" in data:
            continue
        for label, pattern in PATTERNS:
            if pattern.search(data):
                findings.append(f"{path}: {label}")

    if findings:
        print("Potential tracked secrets detected:", file=sys.stderr)
        for finding in findings:
            print(f"  {finding}", file=sys.stderr)
        return 1

    print("secret-scan: no tracked high-confidence credentials found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
