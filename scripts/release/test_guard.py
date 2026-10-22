"""Exercise complete-stream redaction, capture limits, failures and cancellation."""

import base64
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from io import StringIO
import os
from pathlib import Path
import signal
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import guard
import signing

HELPER = Path(signing.__file__).resolve()


class GuardTests(unittest.TestCase):
    @contextmanager
    def directory(self):
        original = Path.cwd()
        with tempfile.TemporaryDirectory() as temp:
            os.chdir(temp)
            try:
                yield Path(temp)
            finally:
                os.chdir(original)

    def test_complete_stream_redaction_and_encoded_forms(self):
        secret = 'release-test $secret\'"\\value'
        inputs = {"RELEASE_GPG_PASSPHRASE": secret}
        forms = guard.secret_forms(inputs)
        # Split each encoded secret across writes and across multiple read chunks.
        program = "import os; secret=os.environ['FIXTURE_SECRET'].encode(); "
        program += "os.write(1, secret[:4]); os.write(1, secret[4:]); "
        program += "import base64,shlex; os.write(2, base64.b64encode(secret)); "
        program += "os.write(1, shlex.quote(secret.decode()).encode())"
        with self.directory(), redirect_stdout(StringIO()) as output:
            status = guard.execute([sys.executable, "-c", program],
                                   {**os.environ, "FIXTURE_SECRET": secret}, inputs)
            self.assertEqual(status, 0)
            log = next(Path("release-logs").glob("*.log")).read_bytes()
            self.assertNotIn(secret.encode(), log)
            self.assertNotIn(base64.b64encode(secret.encode()), log)
            self.assertNotIn(shlex.quote(secret).encode(), log)
            self.assertEqual(output.getvalue().count("[REDACTED]"), 3)
            for form in forms:
                self.assertEqual(guard.sanitize(b"prefix " + form + b" suffix", forms),
                                 b"prefix [REDACTED] suffix")

    def test_overflow_discards_partial_secret_and_stops_command(self):
        with self.directory(), patch.object(guard, "OUTPUT_LIMIT", 16), redirect_stdout(StringIO()) as output:
            with self.assertRaisesRegex(signing.SigningError, "capture limit"):
                guard.execute([sys.executable, "-c", "print('partial-sensitive-value' * 1000)"],
                              dict(os.environ), {})
            self.assertNotIn("partial-sensitive", output.getvalue())
            self.assertIn("withheld", next(Path("release-logs").glob("*.log")).read_text())

    def test_log_write_failure_preserves_failed_command_status(self):
        with self.directory(), patch.object(guard, "publish_logs", side_effect=OSError("secret")), \
                redirect_stderr(StringIO()) as output:
            for exit_status, expected in [(17, 17), (0, 1)]:
                self.assertEqual(guard.execute([sys.executable, "-c", f"raise SystemExit({exit_status})"],
                                              dict(os.environ), {}), expected)
            self.assertNotIn("secret", output.getvalue())

    def test_unsafe_options_rejected_before_process_start(self):
        for argument in ("--verbose", "--debug", "--password=secret", "TOKEN=secret"):
            with self.subTest(argument=argument), patch.object(subprocess, "Popen") as launch:
                with self.assertRaises(signing.SigningError):
                    guard.execute(["builder", argument], {}, {})
                launch.assert_not_called()

    def test_incomplete_output_is_withheld(self):
        program = "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); print('partial-secret')"
        with self.directory(), patch.object(guard, "DRAIN_GRACE", 0.1), redirect_stdout(StringIO()) as output:
            with self.assertRaisesRegex(signing.SigningError, "Incomplete"):
                guard.execute([sys.executable, "-c", program], dict(os.environ), {})
            self.assertNotIn("partial-secret", output.getvalue())

    def test_capture_overflow_preserves_already_failed_command_status(self):
        real_popen = subprocess.Popen
        def completed_command(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            process.wait(timeout=5)
            return process
        with self.directory(), patch.object(guard, "OUTPUT_LIMIT", 16), \
                patch.object(subprocess, "Popen", side_effect=completed_command), redirect_stdout(StringIO()) as output:
            self.assertEqual(guard.execute([sys.executable, "-c", "print('sensitive' * 4); raise SystemExit(17)"],
                                          dict(os.environ), {}), 17)
            self.assertNotIn("sensitive", output.getvalue())

    def test_cancellation_interrupts_blocked_key_import(self):
        self.blocked_key_import(nested=False)

    def test_cancellation_stops_nested_gpg_process(self):
        self.blocked_key_import(nested=True)

    def blocked_key_import(self, nested):
        with self.directory() as directory:
            tools = directory / "bin"
            tools.mkdir()
            fake_gpg = tools / "gpg"
            fake_gpg.write_text("#!" + sys.executable + "\nimport os,pathlib,signal,time\n"
                                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                                "pathlib.Path('gpg-pid').write_text(str(os.getpid()))\n"
                                "time.sleep(30)\n")
            fake_gpg.chmod(0o700)
            env = {key: value for key, value in os.environ.items() if not key.startswith("RELEASE_")}
            env.update(PATH=str(tools) + os.pathsep + env["PATH"], RUNNER_TEMP=str(directory),
                       RELEASE_GPG_PRIVATE_KEY="disposable-test-only-key", RELEASE_GPG_PASSPHRASE="fixture-passphrase")
            command = [sys.executable, str(HELPER), "exec", "true"]
            if nested:
                # Public verification imports the certificate inside a captured
                # release command, putting GPG one ownership level further down.
                command = [sys.executable, str(HELPER), "exec-build", sys.executable,
                           str(HELPER), "verify-commit", "HEAD"]
            process = subprocess.Popen(command,
                                       env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 10
                while not Path("gpg-pid").exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(Path("gpg-pid").exists())
                child = int(Path("gpg-pid").read_text())
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=8)
                self.assertEqual(process.returncode, 143, (stdout, stderr))
                self.assertFalse(list(directory.glob("release-signing-*")))
                stat = Path(f"/proc/{child}/stat")
                self.assertTrue(not stat.exists() or stat.read_text().split()[2] == "Z")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

    def test_signal_during_startup_stops_owned_process(self):
        real_popen = subprocess.Popen
        def interrupt_start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            signal.raise_signal(signal.SIGTERM)
            return process
        with self.directory(), redirect_stdout(StringIO()), patch.object(subprocess, "Popen", side_effect=interrupt_start):
            with self.assertRaises(SystemExit) as result:
                guard.execute([sys.executable, "-c", "import time; time.sleep(30)"], dict(os.environ), {})
            self.assertEqual(result.exception.code, 143)

    def test_cancellation_stops_nested_process_groups(self):
        with self.directory() as directory:
            pidfile = directory / "pid"
            program = "import os,signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
            program += "pathlib.Path('pid').write_text(str(os.getpid())); time.sleep(30)"
            command = [sys.executable, str(HELPER), "exec-build", sys.executable,
                       str(HELPER), "exec-build", sys.executable, "-c", program]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env={key: value for key, value in os.environ.items()
                                            if not key.startswith("RELEASE_")})
            try:
                deadline = time.monotonic() + 10
                while not pidfile.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(pidfile.exists())
                child = int(pidfile.read_text())
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143, (stdout, stderr))
                # A zombie awaits its new parent's reap but cannot execute further.
                stat = Path(f"/proc/{child}/stat")
                self.assertTrue(not stat.exists() or stat.read_text().split()[2] == "Z")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()


if __name__ == "__main__":
    unittest.main()
