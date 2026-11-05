# Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
"""Fail-closed release checkpoints and immutable publication operations.

Only the PyPA action uploads packages. All other publication stages are explicit
commands so failures never fall through to a later stage or restart preparation.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import signing
import version


class ReleaseError(ValueError):
    pass


def command(args, payload=None):
    result = subprocess.run(
        args, input=payload, capture_output=True, text=True, timeout=120
    )
    if result.returncode:
        raise ReleaseError(
            f"{args[0]} operation failed; publication stopped. Inspect the completed stage before recovery."
        )
    return result.stdout


def repository():
    return os.environ["GITHUB_REPOSITORY"]


def api(endpoint, payload=None, paginate=False):
    args = ["gh", "api", "repos/" + repository() + "/" + endpoint]
    if paginate:
        args += ["--paginate", "--slurp"]
    if payload is not None:
        args += ["--method", "POST", "--input", "-"]
    return json.loads(command(args, None if payload is None else json.dumps(payload)))


def listing(endpoint):
    pages = api(endpoint, paginate=True)
    if (
        not isinstance(pages, list)
        or not pages
        or any(not isinstance(page, list) for page in pages)
    ):
        raise ReleaseError("Incomplete paginated API response.")
    return [item for page in pages for item in page]


def blockers():
    # REST issues includes pull requests, while gh issue list excludes them.
    # Search results can be incomplete; enumerate all pages of the issues API.
    items = listing("issues?state=open&labels=release-blocker&per_page=100")
    if any(
        not isinstance(item, dict) or not item.get("html_url") or not item.get("number")
        for item in items
    ):
        raise ReleaseError("Invalid release-blocker response.")
    if items:
        raise ReleaseError(
            "Open release blockers: " + ", ".join(item["html_url"] for item in items)
        )


def check_main(expected):
    lines = command(
        ["git", "ls-remote", "--exit-code", "origin", "refs/heads/main"]
    ).splitlines()
    if lines != [expected + "\trefs/heads/main"]:
        raise ReleaseError(
            "Remote main advanced; stop and reconcile any completed publication manually."
        )


def check_tag_available(tag):
    if command(["git", "ls-remote", "--tags", "origin", "refs/tags/" + tag]).strip():
        raise ReleaseError(
            "Remote release tag already exists; manual recovery required."
        )


def registry_available(registry, normalized):
    host = "pypi.org" if registry == "pypi" else "test.pypi.org"
    try:
        with urlopen(
            f"https://{host}/pypi/alternator-client/{normalized}/json", timeout=30
        ):
            pass
    except HTTPError as error:
        if error.code == 404:
            return
        raise ReleaseError(
            f"Registry availability check failed (HTTP {error.code})."
        ) from None
    raise ReleaseError(
        "Registry version already exists; never reuse a published version."
    )


def releases_for(tag):
    releases = listing("releases?per_page=100")
    if any(
        not isinstance(item, dict)
        or not isinstance(item.get("tag_name"), str)
        or not isinstance(item.get("draft"), bool)
        or not isinstance(item.get("id"), int)
        for item in releases
    ):
        raise ReleaseError("Invalid release discovery response.")
    return [item for item in releases if item["tag_name"] == tag]


def release_available(tag):
    if releases_for(tag):
        raise ReleaseError(
            "Existing GitHub release or draft requires manual recovery; replacement is forbidden."
        )


def packages():
    wheels = sorted(Path("dist").glob("*.whl"))
    sources = sorted(Path("dist").glob("*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise ReleaseError(
            "Expected exactly one pure Python wheel and one source distribution."
        )
    return wheels + sources


def check_metadata(normalized):
    wheel, source = packages()
    if (
        wheel.name != f"alternator_client-{normalized}-py3-none-any.whl"
        or source.name != f"alternator_client-{normalized}.tar.gz"
    ):
        raise ReleaseError("Unexpected distribution filename or native package.")
    with zipfile.ZipFile(wheel) as archive:
        names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(names) != 1:
            raise ReleaseError("Ambiguous wheel metadata.")
        metadata = [archive.read(names[0]).decode()]
    with tarfile.open(source) as archive:
        names = [
            member
            for member in archive.getmembers()
            if member.name == f"alternator_client-{normalized}/PKG-INFO"
        ]
        if len(names) != 1 or not names[0].isfile():
            raise ReleaseError("Ambiguous source metadata.")
        metadata.append(archive.extractfile(names[0]).read().decode())
    for text in metadata:
        fields = Parser().parsestr(text)
        if fields.get_all("Name") != ["alternator-client"] or fields.get_all(
            "Version"
        ) != [normalized]:
            raise ReleaseError(
                "Distribution metadata disagrees with the requested release."
            )


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_artifacts(normalized):
    check_metadata(normalized)
    files = packages() + [Path("dist/SHA256SUMS")]
    expected = "".join(f"{digest(path)}  {path.name}\n" for path in packages())
    if Path("dist/SHA256SUMS").read_text() != expected:
        raise ReleaseError("Release checksums disagree with the distributions.")
    expected_names = {path.name for path in files} | {
        path.name + ".asc" for path in files
    }
    if {path.name for path in Path("dist").iterdir()} != expected_names:
        raise ReleaseError("Unexpected release assets.")
    with signing.public_environment() as env:
        for path in files:
            signing.verify_status(
                signing.gpg(
                    "--status-fd", "1", "--verify", str(path) + ".asc", path, env=env
                )
            )
    return sorted(Path("dist").iterdir())


def identity():
    requested = os.environ["RELEASE_VERSION"]
    return requested, version.normalize(requested), "v" + requested


def manifest():
    requested, normalized, tag = identity()
    signing.verify_prepared()
    commit = command(["git", "rev-parse", "HEAD"]).strip()
    signing.verify_tag(tag, commit)
    assets = verify_artifacts(normalized)
    # Bundle includes commit AND annotated tag, with full history for standalone recovery.
    command(["git", "bundle", "verify", "release-source.bundle"])
    heads = command(
        ["git", "bundle", "list-heads", "release-source.bundle"]
    ).splitlines()
    tag_object = command(["git", "rev-parse", "refs/tags/" + tag]).strip()
    if heads != [commit + " HEAD", tag_object + " refs/tags/" + tag]:
        raise ReleaseError(
            "Recovery bundle does not contain the exact prepared commit and tag."
        )
    files = assets + [Path("release-source.bundle")]
    data = {
        "schema": 1,
        "baseline": os.environ["RELEASE_BASE_COMMIT"],
        "commit": commit,
        "tag": tag,
        "tag_object": tag_object,
        "version": requested,
        "package_version": normalized,
        "repository": repository(),
        "registry": os.environ["RELEASE_REGISTRY"],
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "sha256": {str(path): digest(path) for path in files},
    }
    Path("release-manifest.json").write_text(json.dumps(data, indent=2) + "\n")
    # Only verified distributions enter the PyPA action; retain signatures intact.
    Path("packages").mkdir(exist_ok=False)
    for path in packages():
        shutil.copyfile(path, Path("packages") / path.name)


def verify_recovery(after_upload=False):
    data = json.loads(Path("release-manifest.json").read_text())
    requested, normalized, tag = identity()
    if (
        data["schema"] != 1
        or data["version"] != requested
        or data["package_version"] != normalized
        or data["tag"] != tag
        or data["baseline"] != os.environ["RELEASE_BASE_COMMIT"]
        or data["repository"] != repository()
        or data["registry"] != os.environ["RELEASE_REGISTRY"]
        or data["commit"] != command(["git", "rev-parse", "HEAD"]).strip()
        or data["tag_object"]
        != command(["git", "rev-parse", "refs/tags/" + tag]).strip()
    ):
        raise ReleaseError("Recovery manifest identity mismatch.")
    signing.verify_prepared()
    signing.verify_tag(tag, data["commit"])
    assets = verify_artifacts(normalized) + [Path("release-source.bundle")]
    if data["sha256"] != {str(path): digest(path) for path in assets}:
        raise ReleaseError("Recovery artifact hashes disagree with the manifest.")
    names = {path.name for path in packages()}
    actual = {path.name for path in Path("packages").iterdir()}
    # The pinned PyPA action writes PEP 740 attestations alongside the original
    # distributions. Permit only their exact derived names after upload; hashes
    # of the signed distributions remain mandatory at every publication stage.
    allowed = names | (
        {name + ".publish.attestation" for name in names} if after_upload else set()
    )
    if not names <= actual <= allowed:
        raise ReleaseError("Unexpected registry upload files.")
    for path in packages():
        if digest(Path("packages") / path.name) != digest(path):
            raise ReleaseError("Registry upload differs from the signed distribution.")
    return data


def checkpoint(registry, production):
    requested, normalized, tag = identity()
    if production:
        blockers()
        release_available(tag)
        check_tag_available(tag)
    registry_available(registry, normalized)
    verify_recovery()
    # Last operation before the PyPA action starts the registry upload.
    check_main(os.environ["RELEASE_BASE_COMMIT"])


def push():
    data = verify_recovery(after_upload=True)
    if data["registry"] != "pypi":
        raise ReleaseError(
            "TestPyPI preparations cannot write remote Git refs or GitHub releases."
        )
    blockers()
    check_main(data["baseline"])
    # Exact lease also rejects main advancing during the upload or push handshake.
    command(
        [
            "git",
            "-c",
            "credential.helper=",
            "-c",
            "credential.helper=!gh auth git-credential",
            "push",
            "--atomic",
            "--force-with-lease=refs/heads/main:" + data["baseline"],
            "origin",
            "HEAD:refs/heads/main",
            "refs/tags/" + data["tag"],
        ]
    )


def release_body(tag, normalized):
    notes = api("releases/generate-notes", {"tag_name": tag})["body"]
    return (
        notes
        + f'''

## Installation

```bash
pip install alternator-client=={normalized}
```

## Verification

Download the distributions, signatures and checksums into an empty directory:

```bash
mkdir "alternator-client-{tag}"
cd "alternator-client-{tag}"
gh release download "{tag}" --repo "{repository()}"
curl -fsSL https://github.com/scylladb-publisher.gpg | gpg --import
gpg --fingerprint {signing.PRIMARY}
gpg --verify SHA256SUMS.asc SHA256SUMS
sha256sum --check --strict SHA256SUMS
```

Require certification fingerprint `{signing.PRIMARY}` and signing fingerprint
`{signing.SIGNING}`. Each distribution also has a detached `.asc` signature.
'''
    )


def github_release():
    data = verify_recovery(after_upload=True)
    if data["registry"] != "pypi":
        raise ReleaseError(
            "TestPyPI preparations cannot write remote Git refs or GitHub releases."
        )
    blockers()
    release_available(data["tag"])
    remote = command(
        ["git", "ls-remote", "--tags", "origin", "refs/tags/" + data["tag"]]
    ).strip()
    if remote != data["tag_object"] + "\trefs/tags/" + data["tag"]:
        raise ReleaseError("Remote tag differs from the verified prepared tag.")
    body = release_body(data["tag"], data["package_version"])
    blockers()
    created = api(
        "releases",
        {
            "tag_name": data["tag"],
            "target_commitish": data["commit"],
            "name": data["tag"],
            "draft": True,
            "prerelease": data["package_version"] != data["version"]
            or any(c.isalpha() for c in data["package_version"]),
            "body": body,
        },
    )
    if (
        not isinstance(created.get("id"), int)
        or created.get("draft") is not True
        or created.get("tag_name") != data["tag"]
    ):
        raise ReleaseError("Uncertain draft creation; manual recovery required.")
    release_id = created["id"]
    # Save the exact draft identity before attachment. Do not rediscover and adopt another draft.
    Path("release-draft.json").write_text(json.dumps(created, indent=2) + "\n")
    blockers()
    command(
        [
            "gh",
            "release",
            "upload",
            data["tag"],
            "--repo",
            repository(),
            *[str(path) for path in sorted(Path("dist").iterdir())],
        ]
    )
    assets = listing(f"releases/{release_id}/assets?per_page=100")
    expected = {
        path.name: (path.stat().st_size, "sha256:" + digest(path))
        for path in Path("dist").iterdir()
    }
    if (
        len(assets) != len(expected)
        or {asset["name"]: (asset["size"], asset.get("digest")) for asset in assets}
        != expected
    ):
        raise ReleaseError(
            "Draft assets differ from verified distributions; manual recovery required."
        )
    blockers()
    verify_recovery(after_upload=True)
    # The only transition that publishes; immutable releases lock here.
    command(
        [
            "gh",
            "api",
            "--method",
            "PATCH",
            "repos/" + repository() + f"/releases/{release_id}",
            "--input",
            "-",
        ],
        json.dumps({"draft": False}),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=[
            "blockers",
            "main",
            "available",
            "metadata",
            "manifest",
            "verify",
            "checkpoint",
            "push",
            "github",
        ],
    )
    parser.add_argument("--registry", choices=["pypi", "testpypi"], default="pypi")
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args()
    try:
        if args.operation == "blockers":
            blockers()
        elif args.operation == "main":
            check_main(os.environ["RELEASE_BASE_COMMIT"])
        elif args.operation == "available":
            requested, normalized, tag = identity()
            registry_available(args.registry, normalized)
            if args.production:
                check_tag_available(tag)
                release_available(tag)
        elif args.operation == "metadata":
            check_metadata(version.normalize(os.environ["RELEASE_VERSION"]))
        elif args.operation == "manifest":
            manifest()
        elif args.operation == "verify":
            verify_recovery()
        elif args.operation == "checkpoint":
            checkpoint(args.registry, args.production)
        elif args.operation == "push":
            push()
        else:
            github_release()
    except (ReleaseError, signing.SigningError) as error:
        parser.exit(1, str(error) + "\n")
    except Exception:
        parser.exit(
            1,
            "Release operation failed; values withheld. Inspect completed stages before manual recovery.\n",
        )


if __name__ == "__main__":
    main()
