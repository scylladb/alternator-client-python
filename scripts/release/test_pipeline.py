# Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
"""Fault injection for release gates and irreversible publication stages."""

import io
import os
import tarfile
import tempfile
import unittest
import zipfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import pipeline
import version


@contextmanager
def workspace():
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as directory:
        os.chdir(directory)
        try:
            yield Path(directory)
        finally:
            os.chdir(original)


class GateTests(unittest.TestCase):
    def test_blockers_include_paginated_prs_and_fail_closed(self):
        with patch.dict(os.environ, GITHUB_REPOSITORY="owner/repo"):
            responses = [
                "[[], []]",
                '[[], [{"number": 42, "html_url": "https://example/PR/42", "pull_request": {}}]]',
                "null",
                "[{}]",
                "[[{}]]",
                "{}",
            ]
            for index, response in enumerate(responses):
                with (
                    self.subTest(response=response),
                    patch.object(pipeline, "command", return_value=response) as command,
                ):
                    if index == 0:
                        pipeline.blockers()
                    else:
                        with self.assertRaises(pipeline.ReleaseError):
                            pipeline.blockers()
                    self.assertIn("--paginate", command.call_args.args[0])
                    self.assertIn("--slurp", command.call_args.args[0])
                    self.assertIn(
                        "issues?state=open&labels=release-blocker",
                        command.call_args.args[0][2],
                    )
            with (
                patch.object(
                    pipeline,
                    "command",
                    side_effect=pipeline.ReleaseError("API unavailable"),
                ),
                self.assertRaises(pipeline.ReleaseError),
            ):
                pipeline.blockers()

    def test_main_advance_or_missing_remote_stops(self):
        for result in ["later\trefs/heads/main\n", "", "base\trefs/heads/other\n"]:
            with (
                patch.object(pipeline, "command", return_value=result),
                self.assertRaises(pipeline.ReleaseError),
            ):
                pipeline.check_main("base")
        with patch.object(pipeline, "command", return_value="base\trefs/heads/main\n"):
            pipeline.check_main("base")

    def test_registry_rejects_existing_and_errors(self):
        for registry, host in [("pypi", "pypi.org"), ("testpypi", "test.pypi.org")]:
            for code in [200, 404, 403, 500]:
                error = (
                    None
                    if code == 200
                    else HTTPError("url", code, "failed", None, None)
                )
                with (
                    self.subTest(registry=registry, code=code),
                    patch.object(pipeline, "urlopen", side_effect=error) as request,
                ):
                    if code == 404:
                        pipeline.registry_available(registry, "2.0.2rc1")
                    else:
                        with self.assertRaises(pipeline.ReleaseError):
                            pipeline.registry_available(registry, "2.0.2rc1")
                    self.assertEqual(
                        request.call_args.args[0],
                        f"https://{host}/pypi/alternator-client/2.0.2rc1/json",
                    )

    def test_existing_drafts_releases_or_discovery_failure_stop(self):
        for response in [
            [[{"id": 1, "tag_name": "v1.0.0", "draft": True}]],
            [[{"id": 1, "tag_name": "v1.0.0", "draft": False}]],
            None,
            [[], [{}]],
        ]:
            with (
                self.subTest(response=response),
                patch.object(pipeline, "api", return_value=response),
                self.assertRaises(pipeline.ReleaseError),
            ):
                pipeline.release_available("v1.0.0")
        with (
            patch.object(pipeline, "api", side_effect=OSError("API failure")),
            self.assertRaises(OSError),
        ):
            pipeline.release_available("v1.0.0")

    def test_metadata_synchronized_and_rejects_ambiguous_native_without_writes(self):
        with workspace(), patch.dict(os.environ, {}, clear=True):
            Path("alternator").mkdir()
            Path("native").mkdir()
            Path(version.FILES[0]).write_text('__version__ = "1.0.0"\n')
            Path(version.FILES[1]).write_text(
                '[project]\nversion = "1.0.0"\n[build-system]\nversion = "unrelated"\n'
            )
            self.assertEqual(version.set_versions("2.0.0-rc.1"), "2.0.0rc1")
            self.assertIn('"2.0.0rc1"', Path(version.FILES[0]).read_text())
            self.assertIn('version = "2.0.0rc1"', Path(version.FILES[1]).read_text())
            self.assertIn('version = "unrelated"', Path(version.FILES[1]).read_text())
            Path(version.FILES[1]).write_text("[project]\n")
            with self.assertRaises(ValueError):
                version.set_versions("3.0.0")
            self.assertIn('"2.0.0rc1"', Path(version.FILES[0]).read_text())

    def test_checkpoint_final_operation_rechecks_main(self):
        for production in [True, False]:
            calls = []
            with (
                patch.dict(
                    os.environ, RELEASE_VERSION="1.0.0", RELEASE_BASE_COMMIT="base"
                ),
                patch.multiple(
                    pipeline,
                    **{
                        name: Mock(side_effect=lambda *a, n=name: calls.append(n))
                        for name in [
                            "blockers",
                            "release_available",
                            "check_tag_available",
                            "registry_available",
                            "verify_recovery",
                            "check_main",
                        ]
                    },
                ),
            ):
                pipeline.checkpoint("pypi", production)
            self.assertEqual(calls[-1], "check_main")
            self.assertEqual("blockers" in calls, production)

    def test_main_advance_at_final_upload_checkpoint_stops_release(self):
        for production in (True, False):
            with (
                patch.dict(
                    os.environ, RELEASE_VERSION="1.0.0", RELEASE_BASE_COMMIT="base"
                ),
                patch.multiple(
                    pipeline,
                    blockers=Mock(),
                    release_available=Mock(),
                    check_tag_available=Mock(),
                    registry_available=Mock(),
                    verify_recovery=Mock(),
                ),
                patch.object(
                    pipeline, "command", return_value="advanced\trefs/heads/main\n"
                ),
                self.assertRaisesRegex(pipeline.ReleaseError, "Remote main advanced"),
            ):
                pipeline.checkpoint("pypi" if production else "testpypi", production)

    def test_testpypi_cannot_write_git_or_github(self):
        for operation in (pipeline.push, pipeline.github_release):
            with (
                patch.object(
                    pipeline, "verify_recovery", return_value={"registry": "testpypi"}
                ),
                patch.object(pipeline, "command") as command,
                self.assertRaises(pipeline.ReleaseError),
            ):
                operation()
            command.assert_not_called()

    def test_push_uses_exact_lease_and_atomic_and_stops_on_advance(self):
        data = {"baseline": "base", "tag": "v1.0.0", "registry": "pypi"}
        for advances in [False, True]:
            with (
                patch.object(pipeline, "verify_recovery", return_value=data),
                patch.object(pipeline, "blockers"),
                patch.object(
                    pipeline,
                    "check_main",
                    side_effect=pipeline.ReleaseError("advanced") if advances else None,
                ),
                patch.object(pipeline, "command") as command,
            ):
                if advances:
                    with self.assertRaises(pipeline.ReleaseError):
                        pipeline.push()
                    command.assert_not_called()
                else:
                    pipeline.push()
                    self.assertEqual(
                        command.call_args.args[0],
                        [
                            "git",
                            "-c",
                            "credential.helper=",
                            "-c",
                            "credential.helper=!gh auth git-credential",
                            "push",
                            "--atomic",
                            "--force-with-lease=refs/heads/main:base",
                            "origin",
                            "HEAD:refs/heads/main",
                            "refs/tags/v1.0.0",
                        ],
                    )

    def test_github_draft_attachment_and_publication_order_and_failure(self):
        for failure in [None, "discovery", "create", "upload", "assets", "blocker"]:
            with (
                self.subTest(failure=failure),
                workspace(),
                patch.dict(os.environ, GITHUB_REPOSITORY="owner/repo"),
            ):
                Path("dist").mkdir()
                Path("dist/package.whl").write_text("fixture")
                data = {
                    "tag": "v1.0.0",
                    "version": "1.0.0",
                    "package_version": "1.0.0",
                    "commit": "prepared",
                    "tag_object": "tag-object",
                    "registry": "pypi",
                }
                calls = []

                def command(args, payload=None):
                    calls.append(args)
                    if "ls-remote" in args:
                        return "tag-object\trefs/tags/v1.0.0\n"
                    if failure == "upload" and "upload" in args:
                        raise pipeline.ReleaseError("upload failed")
                    return ""

                def api(endpoint, payload=None):
                    calls.append([endpoint, payload])
                    if failure == "create":
                        raise pipeline.ReleaseError("uncertain creation")
                    return {"id": 9, "tag_name": "v1.0.0", "draft": True}

                def listing(endpoint):
                    calls.append([endpoint])
                    return (
                        []
                        if failure == "assets"
                        else [
                            {
                                "name": "package.whl",
                                "size": 7,
                                "digest": "sha256:"
                                + pipeline.digest("dist/package.whl"),
                            }
                        ]
                    )

                with (
                    patch.object(pipeline, "verify_recovery", return_value=data),
                    patch.object(
                        pipeline,
                        "blockers",
                        side_effect=pipeline.ReleaseError("blocker")
                        if failure == "blocker"
                        else None,
                    ),
                    patch.object(
                        pipeline,
                        "release_available",
                        side_effect=pipeline.ReleaseError("discovery")
                        if failure == "discovery"
                        else None,
                    ),
                    patch.object(pipeline, "release_body", return_value="notes"),
                    patch.object(pipeline, "api", side_effect=api),
                    patch.object(pipeline, "listing", side_effect=listing),
                    patch.object(pipeline, "command", side_effect=command),
                ):
                    if failure:
                        with self.assertRaises(pipeline.ReleaseError):
                            pipeline.github_release()
                        self.assertFalse(any("PATCH" in call for call in calls))
                    else:
                        pipeline.github_release()
                        self.assertEqual(
                            calls[-1][:4], ["gh", "api", "--method", "PATCH"]
                        )
                        self.assertEqual(calls[1][1]["draft"], True)
                        self.assertIn("upload", calls[2])
                        self.assertNotIn("--clobber", calls[2])

    def test_real_unsigned_package_metadata_and_tampered_hashes(self):
        with workspace():
            Path("dist").mkdir()
            wheel = Path("dist/alternator_client-1.0.0-py3-none-any.whl")
            source = Path("dist/alternator_client-1.0.0.tar.gz")
            metadata = b"Name: alternator-client\nVersion: 1.0.0\n"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("alternator_client-1.0.0.dist-info/METADATA", metadata)
            with tarfile.open(source, "w:gz") as archive:
                item = tarfile.TarInfo("alternator_client-1.0.0/PKG-INFO")
                item.size = len(metadata)
                archive.addfile(item, io.BytesIO(metadata))
            pipeline.check_metadata("1.0.0")
            with self.assertRaises(pipeline.ReleaseError):
                pipeline.check_metadata("2.0.0")
            Path("dist/SHA256SUMS").write_text("tampered")
            with self.assertRaisesRegex(pipeline.ReleaseError, "checksums"):
                pipeline.verify_artifacts("1.0.0")


if __name__ == "__main__":
    unittest.main()
