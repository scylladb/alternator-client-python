# Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
"""Release input normalization and synchronized Python package metadata."""

import os
import re
import sys
from pathlib import Path

NUMBER = r"(0|[1-9][0-9]*)"
VERSION = re.compile(
    rf"{NUMBER}\.{NUMBER}\.{NUMBER}((-(alpha|beta|rc)\.|a|b|rc){NUMBER})?"
)
FILES = ("alternator/_version.py", "native/pyproject.toml")


def normalize(version):
    if not VERSION.fullmatch(version):
        raise ValueError(
            "Expected X.Y.Z, X.Y.Z-rc.1, -alpha.1, -beta.1, or X.Y.Zrc1/a1/b1"
        )
    return version.replace("-alpha.", "a").replace("-beta.", "b").replace("-rc.", "rc")


def updated(source, version, native=False):
    # Limit native edits to project.version; never touch Cargo or build tooling.
    pattern = r'^__version__ = "[^"\n]*"$'
    replacement = f'__version__ = "{version}"'
    if native:
        sections = re.split(r"(?m)^(?=\[)", source)
        project = [
            i for i, section in enumerate(sections) if section.startswith("[project]\n")
        ]
        if len(project) != 1:
            raise ValueError("Expected one native [project] section")
        i = project[0]
        sections[i], count = re.subn(
            r'^version = "[^"\n]*"$', f'version = "{version}"', sections[i], flags=re.M
        )
        if count != 1:
            raise ValueError("Expected one native project.version assignment")
        return "".join(sections)
    result, count = re.subn(pattern, replacement, source, flags=re.M)
    if count != 1:
        raise ValueError("Expected one __version__ assignment")
    return result


def set_versions(version):
    version = normalize(version)
    # VERSION_FILE retains the standalone fixture interface. Normal preparation
    # always updates both files, validating both before writing either one.
    paths = [Path(os.environ.get("VERSION_FILE", FILES[0]))]
    if "VERSION_FILE" not in os.environ or "NATIVE_VERSION_FILE" in os.environ:
        paths.append(Path(os.environ.get("NATIVE_VERSION_FILE", FILES[1])))
    changes = [
        (path, updated(path.read_text(), version, i == 1))
        for i, path in enumerate(paths)
    ]
    for path, content in changes:
        path.write_text(content)
    return version


if __name__ == "__main__":
    try:
        version = set_versions(os.environ.get("RELEASE_VERSION", ""))
        print(f"package-version={version}")
        print("prerelease=" + str(bool(re.search("[a-z]", version))).lower())
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
