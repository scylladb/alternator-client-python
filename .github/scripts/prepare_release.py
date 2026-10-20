# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Set the dispatched release version and emit GitHub Actions outputs."""

import argparse
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("version_file", type=Path)
    args = parser.parse_args()

    number = r"(?:0|[1-9][0-9]*)"
    match = re.fullmatch(
        rf"(?P<base>{number}\.{number}\.{number})"
        rf"(?:(?:-(?P<long_stage>alpha|beta|rc)\.|(?P<stage>a|b|rc))"
        rf"(?P<serial>{number}))?",
        args.version,
    )
    if match is None:
        parser.error("Expected X.Y.Z, X.Y.Z-rc.1, -alpha.1, -beta.1, or X.Y.Zrc1/a1/b1")

    stage = match["stage"] or {"alpha": "a", "beta": "b", "rc": "rc"}.get(
        match["long_stage"]
    )
    version = match["base"]
    if stage:
        version += stage + match["serial"]

    source = args.version_file.read_text()
    updated, count = re.subn(
        r'^__version__ = "[^"]+"$',
        f'__version__ = "{version}"',
        source,
        flags=re.MULTILINE,
    )
    if count != 1:
        parser.error(f"Expected one __version__ assignment in {args.version_file}")
    args.version_file.write_text(updated)
    print(f"package-version={version}")
    print(f"prerelease={str(bool(stage)).lower()}")


if __name__ == "__main__":
    main()
