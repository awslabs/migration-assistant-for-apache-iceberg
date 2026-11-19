#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Keep every package manifest in step with the repo-root VERSION file.

    python3 scripts/sync_version.py          rewrite the manifests to match VERSION
    python3 scripts/sync_version.py --check  exit 1 if any manifest differs (used in CI)

Releasing: edit VERSION, run this script, commit, then tag vX.Y.Z (see README).
The script rewrites the project's own version in each pyproject.toml, uv.lock,
package.json and package-lock.json. It does not touch dependency versions.
"""
import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")

PYPROJECT = r'(?m)^(version = ")([^"]+)(")$'


def uv_lock(project: str) -> str:
    """The project's own entry in a uv.lock file."""
    return rf'(\[\[package\]\]\nname = "{re.escape(project)}"\nversion = ")([^"]+)(")'


# (file, regex with three groups: prefix, version, suffix; exact number of matches expected)
TARGETS = [
    ("engine/pyproject.toml", PYPROJECT, 1),
    ("agent/pyproject.toml", PYPROJECT, 1),
    ("app/server/pyproject.toml", PYPROJECT, 1),
    ("engine/uv.lock", uv_lock("ima-engine"), 1),
    ("agent/uv.lock", uv_lock("ima-agent"), 1),
    ("app/server/uv.lock", uv_lock("ima-app-server"), 1),
    ("app/client/package.json", r'(?m)^(  "version": ")([^"]+)(")', 1),
    # package-lock.json repeats the version at the top and under packages[""].
    ("app/client/package-lock.json", r'(?m)^((?:  |      )"version": ")([^"]+)(")(?=,\n  "lockfileVersion"|,\n      "dependencies")', 2),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="only report drift; change nothing")
    args = parser.parse_args()

    version = (ROOT / "VERSION").read_text().strip()
    if not SEMVER.match(version):
        print(f"VERSION is not a valid version: {version!r}", file=sys.stderr)
        return 2

    drift = []
    for rel, pattern, expected in TARGETS:
        path = ROOT / rel
        text = path.read_text()
        found = re.findall(pattern, text)
        if len(found) != expected:
            print(f"{rel}: expected {expected} version field(s), found {len(found)}", file=sys.stderr)
            return 2
        if all(v == version for _, v, _ in found):
            continue
        drift.append((rel, sorted({v for _, v, _ in found})))
        if not args.check:
            path.write_text(re.sub(pattern, rf"\g<1>{version}\g<3>", text))

    if drift:
        for rel, versions in drift:
            print(f"{rel}: {', '.join(versions)} (VERSION is {version})")
        if args.check:
            print("Version drift. Run: python3 scripts/sync_version.py", file=sys.stderr)
            return 1
        print(f"Updated {len(drift)} file(s) to {version}.")
    else:
        print(f"All manifests are at {version}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
