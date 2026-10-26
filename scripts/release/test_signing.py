"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import signing

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_PRIMARY = signing.PRIMARY
REPOSITORY_SIGNING = signing.SIGNING


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="repo-signing-fixture-")
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.password = "test-only-release-passphrase"
        def gpg(*args):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=cls.password + "\n", capture_output=True, text=True, check=True, timeout=60,
            )
            return result.stdout
        gpg("--quick-generate-key", "Release Test <release@example.invalid>", "rsa2048", "cert", "1d")
        cls.primary = [line.split(":")[9] for line in
                       gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.primary, "rsa2048", "sign", "1d")
        cls.subkey = [line.split(":")[9] for line in
                      gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][1]
        cls.public = cls.directory / "public.asc"
        cls.public.write_text(gpg("--armor", "--export", cls.primary))
        cls.private = cls.directory / "subkey.asc"
        cls.private.write_text(gpg("--armor", "--export-secret-subkeys", cls.primary))
        cls.full = cls.directory / "full.asc"
        cls.full.write_text(gpg("--armor", "--export-secret-keys", cls.primary))

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["gpgconf", "--homedir", str(cls.keyring), "--kill", "gpg-agent"],
                       capture_output=True, check=True, timeout=60)
        cls.fixture.cleanup()

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-test-") as temp:
            directory = Path(temp)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "GITHUB_", "RELEASE_", "MAKE"))}
            env.update(RUNNER_TEMP=str(directory), GITHUB_ENV=str(directory / "github-env"),
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(directory / "gitconfig"),
                       RELEASE_GPG_PRIVATE_KEY_FILE=str(self.full if full else self.private),
                       RELEASE_GPG_PASSPHRASE=self.password if password is None else password)
            Path(env["GIT_CONFIG_GLOBAL"]).write_text("")
            with patch.dict(os.environ, env, clear=True), \
                    patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                   PUBLIC_KEY=self.public):
                repo = directory / "source"
                signing.run(["git", "init", "--quiet", str(repo)])
                os.chdir(repo)
                try:
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    yield directory
                finally:
                    os.chdir(original)

    @contextmanager
    def session(self):
        with signing.signing_session() as env, patch.dict(os.environ, env, clear=True):
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY", env)
            self.assertNotIn("RELEASE_GPG_PRIVATE_KEY_FILE", env)
            yield env

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        signing.run(["git", "tag", "-s", "-m", "Release 1.0", "v1.0.0"])

    def test_repository_setup_failure_restores_working_directory(self):
        original = Path.cwd()
        real_run = signing.run
        def fail_commit(command, env=None):
            if "commit" in command:
                raise signing.SigningError("fixture commit failed")
            return real_run(command, env)
        with patch.object(signing, "run", side_effect=fail_commit):
            with self.assertRaisesRegex(signing.SigningError, "fixture commit failed"):
                with self.repository():
                    self.fail("Initial commit failure accepted")
        self.assertEqual(Path.cwd(), original)
        with self.repository():
            self.assertTrue(Path(".git").is_dir())

    def test_signed_commit_and_tag_verify_after_session(self):
        with self.repository() as directory:
            # Existing checkout settings must not override the session.
            signing.run(["git", "config", "user.signingkey", "invalid-key"])
            config = Path(".git/config").read_bytes()
            with self.session():
                self.release()
            signing.verify_commit("HEAD")
            signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertFalse(Path(os.environ["GITHUB_ENV"]).exists())

    def test_prepared_history_requires_original_baseline(self):
        with self.repository():
            baseline = signing.run(["git", "rev-parse", "HEAD"]).strip()
            with self.session():
                self.release()
            signing.verify_prepared(baseline)
            with self.assertRaisesRegex(signing.SigningError, "RELEASE_BASE_COMMIT"):
                signing.verify_prepared()
            with self.session():
                signing.run(["git", "commit", "--allow-empty", "-qm", "Extra commit"])
            with self.assertRaisesRegex(signing.SigningError, "exactly one"):
                signing.verify_prepared(baseline)
            signing.run(["git", "reset", "--hard", baseline])
            signing.run(["git", "-c", "user.name=Unsigned", "-c", "user.email=test@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_prepared(baseline)

    def test_invalid_signature_statuses_rejected_even_with_validsig(self):
        valid = ("[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Test\n"
                 + "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY)
        for record in ("EXPKEYSIG", "REVKEYSIG", "EXPSIG", "BADSIG", "ERRSIG", "FAILURE", "ERROR",
                       "KEYEXPIRED", "KEYREVOKED", "SIGEXPIRED"):
            with self.subTest(record=record), self.assertRaises(signing.SigningError):
                signing.verify_status(valid + "\n[GNUPG:] " + record)
        for invalid in (valid.split("\n")[1], valid.replace(signing.SIGNING[-16:] + " Test", "0" * 16 + " Test"),
                        valid + "\n" + valid.split("\n")[0]):
            with self.assertRaises(signing.SigningError):
                signing.verify_status(invalid)

    def test_cleanup_failure_preserves_command_exit_status(self):
        with self.repository() as directory:
            real_stop = signing.stop_agent
            def fail_shutdown(home):
                # Stop the real agent first so this failure injection leaves none behind.
                real_stop(home)
                with patch.object(signing, "run", side_effect=signing.SigningError("unsafe-value")):
                    real_stop(home)
            with patch.object(signing, "stop_agent", side_effect=fail_shutdown), redirect_stderr(StringIO()), \
                    redirect_stdout(StringIO()), patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", "raise SystemExit(19)"]):
                self.assertEqual(signing.main(), 19)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_cancellation_during_key_import_cleans_session(self):
        import signal
        with self.repository() as directory:
            real_gpg = signing.credential_gpg
            def interrupt_import(args, **kwargs):
                result = real_gpg(args, **kwargs)
                if "--import" in args:
                    signal.raise_signal(signal.SIGTERM)
                return result
            with patch.object(signing, "credential_gpg", side_effect=interrupt_import), \
                    patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", "raise SystemExit(7)"]):
                self.assertEqual(signing.main(), 143)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"), self.session():
                self.fail("Empty passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError), self.session():
                self.fail("Wrong passphrase accepted")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_unprotected_subkey_rejected_after_first_use(self):
        with self.repository() as directory:
            keyring = directory / "unprotected"
            keyring.mkdir(mode=0o700)
            def unprotected_gpg(*args):
                return subprocess.run(["gpg", "--no-options", "--homedir", str(keyring), "--batch",
                                       "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                                      input="\n", text=True, capture_output=True, check=True, timeout=60).stdout
            try:
                unprotected_gpg("--quick-generate-key", "Unprotected <test@example.invalid>", "ed25519", "cert", "1d")
                primary = [line.split(":")[9] for line in unprotected_gpg("--with-colons", "--list-keys").splitlines()
                           if line.startswith("fpr:")][0]
                unprotected_gpg("--quick-add-key", primary, "ed25519", "sign", "1d")
                subkey = [line.split(":")[9] for line in unprotected_gpg("--with-colons", "--list-keys").splitlines()
                          if line.startswith("fpr:")][1]
                private = directory / "unprotected.asc"
                private.write_text(unprotected_gpg("--armor", "--export-secret-subkeys", primary))
                os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = str(private)
                with patch.multiple(signing, PRIMARY=primary, SIGNING=subkey), \
                        self.assertRaisesRegex(signing.SigningError, "protected subkey"), self.session():
                    self.fail("Unprotected signing subkey accepted")
            finally:
                signing.stop_agent(keyring)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"), self.session():
                self.fail("Primary private key accepted")

    def test_unsigned_commit_and_tag_rejected(self):
        with self.repository():
            with self.session():
                signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                             "--allow-empty", "-m", "Unsigned"])
                signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m",
                             "Unsigned", "v1.0.0"])
            with self.assertRaises(signing.SigningError):
                signing.verify_commit("HEAD")
            with self.assertRaises(signing.SigningError):
                signing.verify_tag("v1.0.0")

    def test_tag_at_unexpected_commit_rejected(self):
        with self.repository():
            with self.session():
                self.release()
                signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Later"])
            with self.assertRaisesRegex(signing.SigningError, "unexpected commit"):
                signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    def test_fetched_release_tag_replacement_is_authenticated(self):
        with self.repository() as directory:
            with self.session():
                self.release()
            commit = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signed = signing.run(["git", "rev-parse", "refs/tags/v1.0.0"]).strip()
            remote = directory / "remote.git"
            signing.run(["git", "clone", "--bare", "--quiet", str(Path.cwd()), str(remote)])
            signing.run(["git", "remote", "add", "origin", str(remote)])
            for kind, expected in [("signed", 0), ("lightweight", 1), ("unsigned", 1)]:
                with self.subTest(kind=kind):
                    signing.run(["git", "update-ref", "refs/tags/v1.0.0", signed])
                    if kind == "lightweight":
                        signing.run(["git", "update-ref", "refs/tags/v1.0.0", commit])
                    elif kind == "unsigned":
                        signing.run(["git", "-c", "user.name=Fixture", "-c",
                                     "user.email=fixture@example.invalid", "-c", "tag.gpgsign=false",
                                     "tag", "-f", "-a", "-m", "Unsigned replacement", "v1.0.0", commit])
                    signing.run(["git", "push", "--force", "origin", "refs/tags/v1.0.0"])
                    # Start with a previously verified signed local tag. The fetch must
                    # replace it, and verification must inspect the newly fetched object.
                    signing.run(["git", "update-ref", "refs/tags/v1.0.0", signed])
                    signing.run(["git", "fetch", "--force", "origin",
                                 "refs/tags/v1.0.0:refs/tags/v1.0.0"])
                    self.assertEqual(signing.run(["git", "rev-parse",
                                                 "refs/tags/v1.0.0^{commit}"]).strip(), commit)
                    with patch.object(sys, "argv", ["signing.py", "verify-tag", "v1.0.0", commit]), \
                            redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                        self.assertEqual(signing.dispatch(), expected)

    def test_batch_signing_reuse_and_tamper_rejection(self):
        with self.repository(), self.session():
            files = [Path("package.whl"), Path("package.tar.gz"), Path("SHA256SUMS")]
            for artifact in files:
                artifact.write_bytes(b"release package")
            signing.sign_files(files)
            signatures = [Path(str(artifact) + ".asc").read_bytes() for artifact in files]
            signing.sign_files(files)
            self.assertEqual([Path(str(artifact) + ".asc").read_bytes() for artifact in files], signatures)
            files[0].write_bytes(b"changed package")
            with self.assertRaises(signing.SigningError):
                signing.sign_files(files)

    def test_exec_cleans_up_after_command_failure(self):
        with self.repository() as directory:
            config = Path(".git/config").read_bytes()
            with patch("sys.argv", ["signing.py", "exec", sys.executable, "-c", "raise SystemExit(7)"]):
                self.assertEqual(signing.main(), 7)
            self.assertEqual(Path(".git/config").read_bytes(), config)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_exec_accepts_passphrase_file(self):
        with self.repository() as directory:
            password = directory / "passphrase"
            password.write_text(self.password + "\n")
            os.environ["RELEASE_GPG_PASSPHRASE_FILE"] = str(password)
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            with patch("sys.argv", ["signing.py", "exec", "git", "commit", "--quiet",
                                    "--allow-empty", "-m", "Signed from file credentials"]):
                self.assertEqual(signing.main(), 0)
            signing.verify_commit("HEAD")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_cleanup_removes_files_even_when_agent_shutdown_fails(self):
        with self.repository() as directory:
            real_run = signing.run
            def fail_shutdown(command, env=None, **kwargs):
                real_run(command, env, **kwargs)
                raise signing.SigningError("shutdown failed")
            output = StringIO()
            with patch.object(signing, "run", wraps=real_run) as command, redirect_stderr(output):
                with self.session():
                    command.side_effect = fail_shutdown
            self.assertIn("agent shutdown failed", output.getvalue())
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_git_wrapper_reads_current_passphrase_after_agent_reset(self):
        with self.repository(), self.session():
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = "incorrect-fixture-passphrase"
            with self.assertRaises(signing.SigningError):
                signing.run(["git", "commit", "--allow-empty", "-qm", "Must fail"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = self.password
            signing.run(["git", "commit", "--allow-empty", "-qm", "Correct passphrase"])
            signing.verify_commit("HEAD")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        valid = "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY
        valid = "[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Test\n" + valid
        signing.verify_status(valid)
        for status in [valid.replace(signing.SIGNING, "0" * 40),
                       valid.replace(signing.PRIMARY, "0" * 40), valid + "\n" + valid]:
            with self.assertRaises(signing.SigningError):
                signing.verify_status(status)

    @contextmanager
    def make_repository(self):
        with self.repository() as directory:
            for name in ["Makefile", "scripts/release/signing.py", "scripts/release/guard.py"]:
                target = Path(name)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text((ROOT / name).read_text())
            helper = Path("scripts/release/signing.py")
            helper.write_text(helper.read_text()
                              .replace(REPOSITORY_PRIMARY, self.primary)
                              .replace(REPOSITORY_SIGNING, self.subkey))
            shutil.copyfile(self.public, "scripts/release/publisher.asc")
            Path("alternator").mkdir()
            Path("alternator/_version.py").write_text('__version__ = "0.0.0"\n')
            # Stub package tooling; exercise real Make, Git, GPG, and checksum commands.
            tools = directory / "bin"
            tools.mkdir()
            gh = tools / "gh"
            gh.write_text('#!/bin/sh\nprintf "scylladb-publisher\\n"\n')
            gh.chmod(0o700)
            uvx = tools / "uvx"
            uvx.write_text("#!/usr/bin/env python3\n" + "\n".join([
                "import os, pathlib, sys",
                "assert not any(k.startswith('RELEASE_GPG_') for k in os.environ)",
                "assert 'GNUPGHOME' not in os.environ",
                "assert not any(k.startswith(('GIT_CONFIG_KEY_', 'GIT_CONFIG_VALUE_')) for k in os.environ)",
                "assert 'GIT_CONFIG_COUNT' not in os.environ",
                "assert not list(pathlib.Path(os.environ['RUNNER_TEMP']).glob('release-signing-*'))",
                "assert pathlib.Path('alternator/_version.py').read_text() == '__version__ = \"1.0.0\"\\n'",
                "tool = 'twine' if 'twine' in sys.argv else sys.argv[1]",
                "with open(os.environ['BUILD_OBSERVATIONS'], 'a') as log: log.write(tool + '\\n')",
                "if os.environ.get('FAIL_BUILD'): sys.exit(23)",
                "if 'pyproject-build' in sys.argv:",
                "    pathlib.Path('dist').mkdir(exist_ok=True)",
                "    pathlib.Path('dist/package.whl').write_text('package')",
                "    pathlib.Path('dist/package.tar.gz').write_text('source')",
            ]) + "\n")
            uvx.chmod(0o700)
            os.environ.update(PATH=str(tools) + os.pathsep + os.environ["PATH"],
                              RELEASE_VERSION="1.0.0", GITHUB_OUTPUT=str(directory / "output"),
                              BUILD_OBSERVATIONS=str(directory / "build-observations"))
            # Supply all credential forms together: neither values nor file paths
            # may survive into either package-tool subprocess.
            password = directory / "passphrase"
            password.write_text(self.password + "\n")
            os.environ.update(RELEASE_GPG_PRIVATE_KEY=self.private.read_text(),
                              RELEASE_GPG_PASSPHRASE_FILE=str(password))
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Initial", "-c", "user.email=initial@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Fixture"])
            os.environ["RELEASE_BASE_COMMIT"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            yield directory

    def test_make_release_targets(self):
        with self.make_repository() as directory:
            base = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signing.run(["make", "-n", "release-build"])
            self.assertFalse(Path(os.environ["BUILD_OBSERVATIONS"]).exists())
            signing.run(["make", "release-prepare",
                         "RELEASE_GPG_PASSPHRASE=" + self.password])
            self.assertIn("sha=" + signing.run(["git", "rev-parse", "HEAD"]).strip(),
                          Path(os.environ["GITHUB_OUTPUT"]).read_text())
            self.assertEqual(Path(os.environ["BUILD_OBSERVATIONS"]).read_text(), "--from\ntwine\n")
            self.assertTrue(Path("release-source.bundle").is_file())
            signing.run(["git", "bundle", "verify", "release-source.bundle"])
            subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd="dist", check=True,
                           capture_output=True, timeout=60)
            self.assertEqual(len(list(Path("dist").glob("*.asc"))), 3)
            signing.verify_commit("HEAD")
            signing.run(["make", "release-tag"])
            signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())
            signing.run(["make", "release-tag"])
            # An existing tag must never be reused for another commit.
            signing.run(["git", "reset", "--hard", base])
            result = subprocess.run(["make", "release-tag"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Expected exactly one prepared release commit", result.stderr)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_make_release_rerun_by_different_maintainer(self):
        with self.make_repository() as directory:
            # Workflow recursion must not fall back to local account authorization.
            (directory / "bin" / "gh").write_text("#!/bin/sh\nexit 99\n")
            os.environ.update(GITHUB_ACTIONS="true", GITHUB_ACTOR="first-approved-maintainer",
                              GITHUB_TRIGGERING_ACTOR="second-approved-maintainer")
            signing.run(["make", "release-prepare", "RELEASE_WORKFLOW=1"])
            signing.run(["make", "release-tag", "RELEASE_WORKFLOW=1"])
            signing.verify_tag("v1.0.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    @unittest.skipUnless(os.environ.get("RELEASE_REAL_BUILD_TEST") == "1", "opt-in real Python build smoke test")
    def test_real_python_build_sign_bundle_restore_and_tag(self):
        real_uvx = shutil.which("uvx")
        self.assertIsNotNone(real_uvx)
        with self.make_repository() as directory:
            (directory / "bin" / "uvx").unlink()
            Path("alternator/__init__.py").write_text("")
            Path("pyproject.toml").write_text(
                '[build-system]\nrequires = ["setuptools==80.9.0", "wheel==0.45.1"]\n'
                'build-backend = "setuptools.build_meta"\n'
                '[project]\nname = "release-smoke-fixture"\nversion = "1.0.0"\n'
                'description = "Disposable signing smoke project"\n'
                '[tool.setuptools.packages.find]\ninclude = ["alternator*"]\n')
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "-qm", "Real packaging fixture"])
            os.environ["RELEASE_BASE_COMMIT"] = signing.run(["git", "rev-parse", "HEAD"]).strip()
            signing.run(["make", "release-prepare"])
            prepared = signing.run(["git", "rev-parse", "HEAD"]).strip()
            # Restore the bundle into a separate checkout just as the tag job does.
            clone = directory / "restored"
            signing.run(["git", "clone", "--quiet", ".", str(clone)])
            for name in ("release-source.bundle",):
                shutil.copyfile(name, clone / name)
            original = Path.cwd()
            try:
                os.chdir(clone)
                signing.run(["git", "fetch", "release-source.bundle", "HEAD"])
                signing.verify_prepared()
                signing.run(["make", "release-tag"])
                signing.verify_tag("v1.0.0", prepared)
            finally:
                os.chdir(original)
            with signing.public_environment() as env:
                for artifact in Path("dist").iterdir():
                    if artifact.suffix != ".asc":
                        signing.verify_status(signing.gpg("--status-fd", "1", "--verify",
                                                         str(artifact) + ".asc", artifact, env=env))

    def test_release_commit_validates_inputs_before_signing(self):
        with self.make_repository() as directory:
            gh = directory / "bin" / "gh"
            gh.write_text('#!/bin/sh\nprintf "checked\\n" >> "$RELEASE_ACCOUNT_CHECKS"\n'
                          'printf "scylladb-publisher\\n"\n')
            gh.chmod(0o700)
            checks = directory / "account-checks"
            os.environ.update(GITHUB_ACTIONS="false", RELEASE_VERSION="invalid",
                              RELEASE_ACCOUNT_CHECKS=str(checks))
            result = subprocess.run(["make", "release-commit"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Expected X.Y.Z", result.stderr)
            self.assertNotIn("signing.py exec", result.stdout)
            self.assertEqual(Path("alternator/_version.py").read_text(), '__version__ = "0.0.0"\n')
            self.assertFalse(list(directory.glob("release-signing-*")))
            self.assertEqual(checks.read_text(), "checked\nchecked\n")

            os.environ["RELEASE_VERSION"] = "1.0.0"
            signing.run(["make", "release-commit"])
            self.assertEqual(checks.read_text(), "checked\n" * 5)
            signing.verify_commit("HEAD")
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_release_tag_requires_prepared_commit_and_version(self):
        with self.make_repository() as directory:
            signing.run(["make", "release-commit"])
            prepared = signing.run(["git", "rev-parse", "HEAD"]).strip()
            with self.session():
                signing.run(["git", "commit", "--allow-empty", "-qm", "Later signed commit"])
            signing.verify_commit("HEAD")
            result = subprocess.run(["make", "release-tag"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Expected exactly one prepared release commit", result.stderr)
            self.assertEqual(signing.run(["git", "tag", "--list", "v1.0.0"]), "")
            signing.run(["git", "reset", "--hard", prepared])
            bundle = Path("release-source.bundle")
            content = bundle.read_bytes()
            for invalid in [None, b"not a Git bundle\n"]:
                with self.subTest(bundle=invalid):
                    if invalid is None:
                        bundle.unlink()
                    else:
                        bundle.write_bytes(invalid)
                    result = subprocess.run(["make", "release-tag"], capture_output=True,
                                            text=True, timeout=60)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(signing.run(["git", "tag", "--list", "v1.0.0"]), "")
            bundle.write_bytes(content)
            result = subprocess.run(["make", "release-tag"], env={**os.environ, "RELEASE_VERSION": "2.0.0"},
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Prepared release version does not match RELEASE_VERSION", result.stdout + result.stderr)
            self.assertEqual(signing.run(["git", "tag", "--list", "v2.0.0"]), "")
            signing.run(["make", "release-tag"])
            signing.verify_tag("v1.0.0", prepared)
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_release_build_removes_initial_makeflags_overrides(self):
        with self.make_repository():
            signing.run(["make", "release-commit"])
            overrides = "-- RELEASE_GPG_PASSPHRASE=" + self.password
            overrides += " RELEASE_GPG_PRIVATE_KEY_FILE=" + str(self.private)
            overrides += " RELEASE_GPG_PASSPHRASE_FILE=" + os.environ["RELEASE_GPG_PASSPHRASE_FILE"]
            result = subprocess.run(["make", "release-build", "MAKEFLAGS=" + overrides],
                                    capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(Path(os.environ["BUILD_OBSERVATIONS"]).read_text(), "--from\ntwine\n")
            # Execution flags must still govern the real builder sub-make.
            Path(os.environ["BUILD_OBSERVATIONS"]).unlink()
            signing.run(["make", "release-build", "MAKEFLAGS=n " + overrides])
            self.assertFalse(Path(os.environ["BUILD_OBSERVATIONS"]).exists())

    def test_failed_build_stops_before_artifact_signing(self):
        with self.make_repository() as directory:
            os.environ["FAIL_BUILD"] = "1"
            result = subprocess.run(["make", "release-prepare"], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(Path(os.environ["BUILD_OBSERVATIONS"]).read_text(), "--from\n")
            self.assertNotIn("signing.py sign-files", result.stdout)
            self.assertFalse(Path("dist/SHA256SUMS").exists())
            self.assertFalse(list(Path("dist").glob("*.asc")))
            self.assertFalse(list(directory.glob("release-signing-*")))
            signing.verify_commit("HEAD")


class BuildEnvironmentTests(unittest.TestCase):
    def test_makeflags_separator_and_execution_flags(self):
        for flags, expected in [
            ("-- RELEASE_GPG_PASSPHRASE=secret", ""),
            (" -- RELEASE_GPG_PASSPHRASE=secret", ""),
            ("n -- RELEASE_GPG_PASSPHRASE=secret", "n"),
            ("s -j2 --jobserver-auth=3,4 -- RELEASE_GPG_PASSPHRASE=secret",
             "s -j2 --jobserver-auth=3,4"),
            ("s --warn-undefined-variables", "s --warn-undefined-variables"),
        ]:
            with self.subTest(flags=flags), patch.dict(os.environ, {"MAKEFLAGS": flags}, clear=True):
                self.assertEqual(signing.build_environment()["MAKEFLAGS"], expected)


class DiagnosticTests(unittest.TestCase):
    def test_failed_commands_identify_operations_without_unsafe_values(self):
        sentinel = "DO-NOT-DISCLOSE"
        result = subprocess.CompletedProcess([], 17, sentinel, sentinel)
        with patch.object(subprocess, "run", return_value=result):
            with self.assertRaisesRegex(signing.SigningError, r"Git verify-commit failed \(exit status 17\)") as error:
                signing.run(["git", "verify-commit", sentinel])
            self.assertNotIn(sentinel, str(error.exception))
            with self.assertRaisesRegex(signing.SigningError, r"GPG key import failed \(exit status 17\)") as error:
                signing.credential_gpg(["--import", sentinel], payload=sentinel,
                                       env={"RELEASE_GPG_PASSPHRASE": sentinel})
            self.assertNotIn(sentinel, str(error.exception))

    def test_command_start_and_timeout_errors_withhold_payloads(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (subprocess.TimeoutExpired("DO-NOT-DISCLOSE", 60,
                                                             output="DO-NOT-DISCLOSE"), "timed out")]:
            with self.subTest(category=category), patch.object(subprocess, "run", side_effect=failure):
                with self.assertRaises(signing.SigningError) as error:
                    signing.gpg("--verify", "DO-NOT-DISCLOSE")
                self.assertIn("GPG signature verification", str(error.exception))
                self.assertIn(category, str(error.exception))
                self.assertNotIn("DO-NOT-DISCLOSE", str(error.exception))

    def test_unexpected_failure_identifies_operation_and_safe_category(self):
        for failure, category in [(OSError("DO-NOT-DISCLOSE"), "OS error"),
                                  (RuntimeError("DO-NOT-DISCLOSE"), "internal error")]:
            output = StringIO()
            with patch("sys.argv", ["signing.py", "verify-commit", "HEAD"]), \
                    patch.object(signing, "verify_commit", side_effect=failure), redirect_stderr(output):
                self.assertEqual(signing.main(), 1)
            self.assertIn("verify-commit failed (" + category + ")", output.getvalue())
            self.assertNotIn("DO-NOT-DISCLOSE", output.getvalue())


class WorkflowTests(unittest.TestCase):
    def workflow_jobs(self):
        import re
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        return dict(re.findall(r"^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:|\Z)", workflow, re.M | re.S))

    def test_all_signing_and_publishing_jobs_require_release_environment(self):
        jobs = self.workflow_jobs()
        protected = {"build", "tag-release", "publish-testpypi", "publish", "create-release"}
        self.assertEqual(set(jobs), protected | {"validate"})
        for name in protected:
            with self.subTest(job=name):
                self.assertRegex(jobs[name], r"(?m)^    environment: release$")
                self.assertNotIn("GITHUB_TRIGGERING_ACTOR", jobs[name])
                self.assertNotIn("Require publisher release user", jobs[name])
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertIn('test "$GITHUB_REF" = refs/heads/main', workflow)
        self.assertNotRegex(workflow, r"echo .*RELEASE_GPG_.*GITHUB_ENV")
        self.assertIn('baseline: ${{ github.sha }}', workflow)
        self.assertIn('RELEASE_BASE_COMMIT: ${{ needs.build.outputs.baseline }}', workflow)

    def test_validation_finishes_before_approval_on_separate_runner(self):
        jobs = self.workflow_jobs()
        validation, release = jobs["validate"], jobs["build"]
        self.assertRegex(validation, r"(?m)^    runs-on: ubuntu-latest$")
        self.assertNotIn("environment:", validation)
        self.assertNotIn("secrets.", validation)
        self.assertNotIn("RELEASE_GPG_", validation)
        self.assertNotIn("id-token:", validation)
        for command in ("make test-unit", "make test-integration", "make lint"):
            self.assertIn(command, validation)
            self.assertNotIn(command, release)
        for command in ("make release-commit", "make release-build", "make release-sign"):
            self.assertIn(command, release)
            self.assertNotIn(command, validation)
        self.assertRegex(release, r"(?m)^    needs: validate$")
        self.assertRegex(release, r"(?m)^    runs-on: ubuntu-latest$")
        self.assertNotIn("always()", release.split("    steps:\n", 1)[0])
        self.assertNotIn("continue-on-error:", validation)
        self.assertNotIn("actions/download-artifact", release)
        self.assertIn("ref: ${{ github.sha }}", release)
        self.assertIn("VALIDATED_COMMIT: ${{ needs.validate.outputs.commit }}", release)
        self.assertIn('test "$VALIDATED_COMMIT" = "$GITHUB_SHA"', release)
        self.assertIn('test "$(git rev-parse HEAD)" = "$VALIDATED_COMMIT"', release)
        for output in ("package-version", "prerelease"):
            self.assertIn(output + ": ${{ needs.validate.outputs." + output + " }}", release)

    def test_local_entrypoints_all_authorize_before_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            gh = Path(temp) / "gh"
            gh.write_text('#!/bin/sh\nprintf "unauthorized\\n"\n')
            gh.chmod(0o700)
            for actions, workflow, arguments in [("false", "", []), ("true", "", []),
                                                 ("true", "1", []),
                                                 ("false", "", ["RELEASE_WORKFLOW=1"]),
                                                 ("true", "", ["RELEASE_WORKFLOW=0"])]:
                for target in ("release-check-user", "release-version", "release-prepare",
                               "release-commit", "_release-commit", "release-build",
                               "release-sign", "release-tag", "_release-tag"):
                    with self.subTest(actions=actions, workflow=workflow, arguments=arguments,
                                      target=target):
                        result = subprocess.run(
                            ["make", target, *arguments], cwd=ROOT, capture_output=True, text=True,
                            timeout=60, env={**os.environ, "GITHUB_ACTIONS": actions,
                                             "RELEASE_WORKFLOW": workflow,
                                             "PATH": temp + os.pathsep + os.environ["PATH"]})
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("current: unauthorized", result.stderr)
                        self.assertNotIn("signing.py", result.stdout)


class ReleaseUserTests(unittest.TestCase):
    def test_actions_authorization_does_not_require_publisher_actor(self):
        for actor, triggering in [("approved-maintainer", "approved-maintainer"),
                                  ("approved-maintainer", "another-approved-maintainer")]:
            with self.subTest(actor=actor, triggering=triggering):
                subprocess.run(["make", "release-check-user", "RELEASE_WORKFLOW=1"], cwd=ROOT, check=True,
                               capture_output=True, timeout=60,
                               env={**os.environ, "GITHUB_ACTIONS": "true",
                                    "GITHUB_ACTOR": actor, "GITHUB_TRIGGERING_ACTOR": triggering})

    def test_local_release_uses_authenticated_gh_user(self):
        with tempfile.TemporaryDirectory() as temp:
            gh = Path(temp) / "gh"
            gh.write_text('#!/bin/sh\nprintf "%s\\n" "$TEST_GH_USER"\n')
            gh.chmod(0o700)
            env = {**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                   "GITHUB_ACTIONS": "false", "TEST_GH_USER": "someone-else"}
            result = subprocess.run(["make", "release-prepare"], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("current: someone-else", result.stderr)
            env["TEST_GH_USER"] = "scylladb-publisher"
            subprocess.run(["make", "release-check-user"], cwd=ROOT, env=env, check=True,
                           capture_output=True, timeout=60)
            gh.write_text("#!/bin/sh\nexit 1\n")
            result = subprocess.run(["make", "release-prepare"], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Run gh auth login or gh auth switch", result.stderr)


if __name__ == "__main__":
    unittest.main()
