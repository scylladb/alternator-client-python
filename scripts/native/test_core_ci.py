# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Prove required native fault tests cannot silently skip or lose bindings."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree

import ci
from test_ci import COMMIT, RUN_ID, core_evidence


class CoreEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        source = tempfile.TemporaryDirectory()
        self.addCleanup(source.cleanup)
        self.source = Path(source.name)
        core_evidence(self.directory, self.source)

    def verify(self) -> dict:
        return ci.verify_core_evidence(self.directory, COMMIT, RUN_ID, self.source)

    def test_fixture_support_cannot_escape_fresh_environment(self) -> None:
        workspace = self.directory / "workspace"
        executable = workspace / "env/bin/python"
        with (
            patch.object(
                ci.subprocess,
                "run",
                side_effect=[
                    None,
                    subprocess.CompletedProcess([], 0, stdout=str(self.source)),
                ],
            ),
            self.assertRaisesRegex(ValueError, "fresh environment"),
        ):
            ci.prepare_core_environment(
                self.source,
                executable,
                Path("native.whl"),
                Path("base.whl"),
                "2.0.1",
                workspace,
            )

    def test_fixture_staging_uses_only_tracked_inputs(self) -> None:
        (self.source / "tests").mkdir()
        (self.source / "native/tests").mkdir(parents=True)
        (self.source / "tests/__init__.py").write_text("")
        (self.source / "tests/untracked-cert.pem").write_text("local certificate")
        (self.source / "native/tests/operation_adapter.py").write_text("fixture = True")
        (self.source / "native/tests/test_probe.py").write_text(
            "test source stays in checkout"
        )
        tracked = "tests/__init__.py\0native/tests/operation_adapter.py\0native/tests/test_probe.py\0"
        destination = self.directory / "site"
        with patch.object(ci, "git", return_value=tracked):
            ci.stage_core_fixtures(self.source, destination)
        self.assertTrue((destination / "tests/__init__.py").is_file())
        self.assertTrue((destination / "operation_adapter.py").is_file())
        self.assertFalse((destination / "tests/untracked-cert.pem").exists())
        self.assertFalse((destination / "test_probe.py").exists())

    def test_fixture_staging_cannot_shadow_an_installed_package(self) -> None:
        installed = self.directory / "site/alternator"
        installed.mkdir(parents=True)
        with (
            patch.object(ci, "git", return_value="native/tests/alternator.py\0"),
            self.assertRaisesRegex(ValueError, "shadow installed"),
        ):
            ci.stage_core_fixtures(self.source, installed.parent)

    def test_complete_core_evidence(self) -> None:
        receipt = self.verify()
        self.assertEqual(receipt["tests"]["vendor"]["passed"], 45)
        self.assertEqual(receipt["tests"]["startup"]["passed"], 4)
        self.assertEqual(receipt["tests"]["interface"]["passed"], 4)

    def test_startup_context_is_required_and_source_wheel_bound(self) -> None:
        path = self.directory / "startup-context.json"
        original = path.read_bytes()
        receipt_path = self.directory / "core.json"
        receipt = json.loads(receipt_path.read_bytes())
        for mutation in ("bytes", "source", "wheel"):
            context = json.loads(original)
            if mutation == "bytes":
                path.write_bytes(original + b"\n")
            else:
                if mutation == "source":
                    context["source_files"]["Cargo.toml"] = "0" * 64
                else:
                    context["wheel_sha256"] = "0" * 64
                path.write_bytes(ci.canonical(context))
                receipt_path.write_bytes(
                    ci.canonical({**receipt, "startup_context_sha256": ci.digest(path)})
                )
            with (
                self.subTest(mutation=mutation),
                self.assertRaisesRegex(ValueError, "startup context"),
            ):
                self.verify()
            path.write_bytes(original)
            receipt_path.write_bytes(ci.canonical(receipt))
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.verify()

    def test_each_missing_report_fails(self) -> None:
        (self.directory / "native.xml").unlink()
        with self.assertRaises(FileNotFoundError):
            self.verify()

    def test_skipped_failed_or_errored_cases_fail_for_every_suite(self) -> None:
        for suite in ci.CORE_SUITES:
            path = self.directory / "native.xml"
            original = path.read_bytes()
            for outcome in ("skipped", "failure", "error"):
                tree = ElementTree.parse(path)
                case = next(
                    case
                    for case in tree.getroot().iter("testcase")
                    if case.attrib["classname"] == Path(ci.CORE_SUITES[suite][0]).stem
                )
                ElementTree.SubElement(case, outcome)
                tree.write(path)
                with self.assertRaisesRegex(ValueError, "unsuccessful"):
                    ci.pytest_report(path, suite)
                path.write_bytes(original)

    def test_missing_and_duplicate_testcases_fail(self) -> None:
        for suite in ci.CORE_SUITES:
            path = self.directory / "native.xml"
            original = path.read_bytes()
            tree = ElementTree.parse(path)
            cases = [
                case
                for case in tree.getroot().iter("testcase")
                if case.attrib["classname"] == Path(ci.CORE_SUITES[suite][0]).stem
            ]
            cases[0].set("name", cases[1].attrib["name"])
            tree.write(path)
            with self.assertRaisesRegex(ValueError, "Missing"):
                ci.pytest_report(path, suite)
            path.write_bytes(original)
            tree = ElementTree.parse(path)
            suite_node = next(
                node
                for node in tree.getroot().iter("testsuite")
                if any(
                    case.attrib["classname"] == Path(ci.CORE_SUITES[suite][0]).stem
                    for case in node.iter("testcase")
                )
            )
            suite_node.remove(suite_node.find("testcase"))
            tree.write(path)
            with self.assertRaisesRegex(ValueError, "Missing"):
                ci.pytest_report(path, suite)
            path.write_bytes(original)

    def test_startup_names_require_first_middle_last_and_core(self) -> None:
        path = self.directory / "native.xml"
        tree = ElementTree.parse(path)
        next(
            case
            for case in tree.getroot().iter("testcase")
            if case.attrib["classname"] == "test_startup_cleanup"
        ).set("name", "unrelated")
        tree.write(path)
        with self.assertRaisesRegex(ValueError, "first/middle/last/core"):
            ci.pytest_report(path, "startup")

    def test_vendor_requires_all_optimization_modes(self) -> None:
        path = self.directory / "native.xml"
        tree = ElementTree.parse(path)
        for index, case in enumerate(
            case
            for case in tree.getroot().iter("testcase")
            if case.attrib["classname"] == "test_verify_vendor"
        ):
            case.set("name", f"test_{index}[normal]")
        tree.write(path)
        with self.assertRaisesRegex(ValueError, "three optimization modes"):
            ci.pytest_report(path, "vendor")

    def test_future_native_test_files_are_collected_and_cannot_skip(self) -> None:
        path = self.directory / "native.xml"
        tree = ElementTree.parse(path)
        suite = ElementTree.SubElement(tree.getroot(), "testsuite")
        case = ElementTree.SubElement(
            suite, "testcase", classname="test_future_contract", name="test_operation"
        )
        tree.write(path)
        self.assertEqual(ci.pytest_report(path)["passed"], 54)
        ElementTree.SubElement(case, "skipped")
        tree.write(path)
        with self.assertRaisesRegex(ValueError, "unsuccessful"):
            ci.pytest_report(path)

    def test_extra_cases_in_existing_files_preserve_required_baseline(self) -> None:
        path = self.directory / "native.xml"
        tree = ElementTree.parse(path)
        suite = ElementTree.SubElement(tree.getroot(), "testsuite")
        for name, (filename, count) in ci.CORE_SUITES.items():
            ElementTree.SubElement(
                suite,
                "testcase",
                classname=Path(filename).stem,
                name="test_future_contract",
            )
            tree.write(path)
            self.assertEqual(ci.pytest_report(path, name)["passed"], count + 1)
        self.assertEqual(ci.pytest_report(path)["passed"], 56)

    def test_core_source_run_and_vendor_identity_are_bound(self) -> None:
        path = self.directory / "core.json"
        original = path.read_bytes()
        for field, value in (
            ("commit", "c" * 40),
            ("run_id", "different"),
            ("vendor_provenance_sha256", "c" * 64),
        ):
            receipt = json.loads(original)
            receipt[field] = value
            path.write_bytes(ci.canonical(receipt))
            with self.assertRaisesRegex(ValueError, "source"):
                self.verify()
        path.write_bytes(original)

    def test_altered_core_wheel_fails(self) -> None:
        (self.directory / "core.whl").write_bytes(b"altered")
        with self.assertRaisesRegex(ValueError, "wheel hash"):
            self.verify()

    def test_changed_report_cannot_keep_same_receipt(self) -> None:
        path = self.directory / "native.xml"
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "report proof"):
            self.verify()

    def test_missing_developer_tool_fails_before_probes_can_skip(self) -> None:
        for missing in ("cc", "cargo", "rustc"):
            with (
                patch.object(ci.sys, "platform", "linux"),
                patch.object(
                    ci.shutil,
                    "which",
                    side_effect=lambda name, missing=missing: (
                        None if name == missing else f"/bin/{name}"
                    ),
                ),
                self.assertRaisesRegex(ValueError, "requires cc, cargo and rustc"),
            ):
                ci.require_core_tools()

    def test_non_linux_core_qualification_fails(self) -> None:
        with (
            patch.object(ci.sys, "platform", "darwin"),
            self.assertRaisesRegex(ValueError, "requires Linux"),
        ):
            ci.require_core_tools()


if __name__ == "__main__":
    unittest.main()
