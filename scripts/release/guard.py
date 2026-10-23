"""Bounded release command capture. Only sanitized complete streams leave memory."""

import base64
import codecs
import html
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote, quote_plus

import signing

OUTPUT_LIMIT = 8 * 1024 * 1024
DRAIN_GRACE = 3


def secret_forms(env):
    values = set()
    for name, value in env.items():
        if name.startswith("RELEASE_GPG_") or any(
                part in name.upper() for part in ("TOKEN", "PASSWORD", "PASSPHRASE", "PRIVATE_KEY")):
            if value:
                values.add(value)
            if name.startswith("RELEASE_GPG_") and name.endswith("_FILE"):
                # A missing credential file is an error; never silently lose redaction.
                values.add(Path(value).read_text())
    forms = set()
    for value in values:
        for text in {value, value.rstrip("\r\n"), *value.splitlines()} - {""}:
            raw = text.encode()
            forms.update((raw, base64.b64encode(raw), base64.urlsafe_b64encode(raw),
                          raw.hex().encode(), raw.hex().upper().encode(),
                          quote(text, safe="").encode(), quote_plus(text, safe="").encode(),
                          json.dumps(text, ensure_ascii=True)[1:-1].encode(),
                          codecs.escape_encode(raw)[0], shlex.quote(text).encode(),
                          html.escape(text, quote=True).encode(),
                          ("'" + text.replace("'", "'\\''") + "'").encode()))
    return sorted(forms, key=len, reverse=True)


def validate_command(command, env):
    # Release commands are fixed by Make. Reject debug/verbose modes and secret
    # assignments before launching tools; keep credential values out of argv.
    for arg in command[1:]:
        if (arg.lower() in ("-v", "-vv", "-vvv", "--verbose", "--debug", "--trace")
                or arg.lower().startswith(("--verbose=", "--debug=", "--password", "--token"))
                or ("=" in arg and any(word in arg.split("=", 1)[0].upper()
                                       for word in ("PASSPHRASE", "PASSWORD", "PRIVATE_KEY", "TOKEN")))):
            raise signing.SigningError("Unsafe release command options; values withheld.")
    if any(env.get(name, "").lower() not in ("", "0", "false")
           for name in ("UV_VERBOSE", "TWINE_VERBOSE", "SETUPTOOLS_DEBUG")):
        raise signing.SigningError("Release debug logging is disabled.")


def sanitize(payload, forms):
    for form in forms:
        payload = payload.replace(form, b"[REDACTED]")
    return payload


def publish_logs(streams, forms):
    # Sanitize before opening any output file. Atomic replacement prevents an
    # interrupted write from leaving an apparently complete log artifact.
    payload = b"stdout:\n" + sanitize(bytes(streams[0]), forms)
    payload += b"\nstderr:\n" + sanitize(bytes(streams[1]), forms)
    directory = Path("release-logs")
    directory.mkdir(mode=0o700, exist_ok=True)
    descriptor, filename = tempfile.mkstemp(prefix="command-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        os.replace(filename, str(Path(filename).with_suffix(".log")))
    finally:
        Path(filename).unlink(missing_ok=True)
    sys.stdout.write(payload.decode("utf-8", errors="replace"))
    sys.stdout.flush()


def execute(command, env, inputs):
    validate_command(command, env)
    forms = secret_forms(inputs)
    streams = [bytearray(), bytearray()]
    owns_group = inputs.get("RELEASE_OWNED_PROCESS_GROUP") != "1"
    env = {**env, "RELEASE_OWNED_PROCESS_GROUP": "1"}
    status = 1
    failure = False
    with signing.session_signals() as interrupted:
        process = None
        try:
            interrupted.check()
            process = subprocess.Popen(command, env=env, start_new_session=owns_group,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            interrupted.check()
            with selectors.DefaultSelector() as selector:
                for index, stream in enumerate((process.stdout, process.stderr)):
                    selector.register(stream, selectors.EVENT_READ, index)
                exited_at = None
                total = 0
                while selector.get_map():
                    interrupted.check()
                    if process.poll() is not None:
                        exited_at = exited_at or time.monotonic()
                        if time.monotonic() - exited_at > DRAIN_GRACE:
                            raise signing.SigningError("Incomplete release command output.")
                    for key, _ in selector.select(timeout=0.1):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        total += len(chunk)
                        if total > OUTPUT_LIMIT:
                            raise signing.SigningError("Release command output exceeded the capture limit.")
                        streams[key.data].extend(chunk)
                # A command may close both streams while it is still running.
                while process.poll() is None:
                    interrupted.check()
                    time.sleep(0.05)
                status = process.returncode
                status = status if status >= 0 else 128 - status
                # Stop descendants that closed their streams but outlived the build.
                signing.terminate_process(process, owns_group=owns_group)
        except BaseException as error:
            failure = True
            completed_status = None if process is None else process.poll()
            if process is not None:
                signing.terminate_process(process, interrupted.signum or 15, owns_group=owns_group)
                if process.returncode:
                    status = process.returncode if process.returncode > 0 else 128 - process.returncode
            # Incomplete streams cannot safely be redacted (a secret may have been
            # truncated). Discard them rather than publishing a partial match.
            streams = [bytearray(b"Release command output withheld after capture failure.\n"), bytearray()]
            if isinstance(error, signing.SigningError) and completed_status is not None and completed_status > 0:
                status = completed_status
                print("Release output capture failed; preserving the command failure status.", file=sys.stderr)
            else:
                raise
        finally:
            if process is not None:
                process.stdout.close()
                process.stderr.close()
            try:
                publish_logs(streams, forms)
            except (OSError, ValueError):
                print("Sanitized release log output failed; values withheld.", file=sys.stderr)
                if not failure and status == 0:
                    status = 1
    return status
