# Copyright ScyllaDB, Inc.
"""Reproduce compiled/stock parser CPU, startup and quota-limited HTTP pairs.

Run: uv run python scripts/bench_response_parser.py run --output /tmp/parser-bench
The HTTP peer runs outside each systemd client scope. Both variants use the
current Alternator code with default parameter validation. Only the endpoint
parser factory differs. Raw samples are JSONL; environment is recorded separately.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import gc
import hashlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import threading
import time
import tracemalloc
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from typing import Any

# ruff: noqa: ANN401 -- SDK benchmark values and client internals are dynamic

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import aiobotocore
import boto3
import botocore
from aiobotocore.awsrequest import AioAWSResponse
from botocore.awsrequest import AWSResponse

from alternator import Config, close_client, create_client
from alternator.async_client import close_async_client, create_async_client
from alternator.core.response_parser import _ParserFactory, install_response_parser

SMALL = {"pk": {"S": "key"}, "value": {"S": "x" * 64}}
NESTED = dict(SMALL)
for i in range(16):
    NESTED[f"attr{i}"] = {
        "M": {
            "name": {"S": f"value-{i}"},
            "number": {"N": "123.45"},
            "bytes": {"B": "AAECAwQFBgcICQoLDA0ODw=="},
            "list": {"L": [{"S": "tag"}, {"BOOL": False}, {"NULL": True}]},
        }
    }
NESTED["binary_set"] = {"BS": ["Zmlyc3Q=", "AP8B"]}
KEY = {"pk": {"S": "key"}}
WORKLOADS = {
    "get_nested": ("GetItem", {"TableName": "tbl", "Key": KEY}, {"Item": NESTED}),
    "query25": (
        "Query",
        {
            "TableName": "tbl",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "key"}},
        },
        {"Items": [NESTED] * 25, "Count": 25, "ScannedCount": 25},
    ),
    "scan25": (
        "Scan",
        {"TableName": "tbl"},
        {"Items": [NESTED] * 25, "Count": 25, "ScannedCount": 25},
    ),
    "batch_get25": (
        "BatchGetItem",
        {"RequestItems": {"tbl": {"Keys": [KEY]}}},
        {"Responses": {"tbl": [NESTED] * 25}, "UnprocessedKeys": {}},
    ),
    "put_small": ("PutItem", {"TableName": "tbl", "Item": SMALL}, {}),
    "get_empty": ("GetItem", {"TableName": "tbl", "Key": KEY}, {}),
}
BODIES = {
    name: json.dumps(w[2], separators=(",", ":")).encode()
    for name, w in WORKLOADS.items()
}


def emit(**row: Any) -> None:
    print(json.dumps(row), flush=True)


def cgroup_stat() -> dict[str, int]:
    group = pathlib.Path("/proc/self/cgroup").read_text().strip().split(":", 2)[2]
    path = pathlib.Path("/sys/fs/cgroup") / group.lstrip("/")
    return {
        k: int(v)
        for k, v in (
            line.split() for line in (path / "cpu.stat").read_text().splitlines()
        )
    }


@asynccontextmanager
async def clients(
    args: argparse.Namespace, stub: bool = False
) -> AsyncIterator[dict[str, Any]]:
    async with AsyncExitStack() as stack:
        result = {}
        response_class = AioAWSResponse if args.asynchronous else AWSResponse
        for variant in ("stock", "compiled"):
            config = Config(seed_hosts=["127.0.0.1"], port=args.port)
            client = (
                await create_async_client(config)
                if args.asynchronous
                else create_client(config)
            )
            if args.asynchronous:
                stack.push_async_callback(close_async_client, client)
            else:
                stack.callback(close_client, client)
            factory = client._endpoint._response_parser_factory
            assert isinstance(factory, _ParserFactory)
            assert client.meta.config.parameter_validation
            if variant == "stock":
                client._endpoint._response_parser_factory = factory._original
            if stub:

                def send(
                    request: Any, body: bytes = BODIES[args.workload]
                ) -> AWSResponse:
                    r = response_class(
                        request.url, 200, {"x-amzn-requestid": "bench"}, None
                    )
                    r._content = body
                    return r

                async def async_send(request: Any) -> AWSResponse:
                    return send(request)

                client._endpoint.http_session.send = (
                    async_send if args.asynchronous else send
                )

            def header(request: Any, **kwargs: Any) -> None:
                request.headers["x-benchmark-workload"] = args.workload

            client.meta.events.register("before-send.dynamodb", header)
            result[variant] = client
        yield result


def order(trial: int) -> tuple[str, str]:
    return ("stock", "compiled") if trial % 2 == 0 else ("compiled", "stock")


async def cpu(args: argparse.Namespace) -> None:
    os.sched_setaffinity(
        0, {args.cpu if args.cpu is not None else min(os.sched_getaffinity(0))}
    )
    async with clients(args, stub=True) as pair:
        operation, params, _ = WORKLOADS[args.workload]

        async def call(client: Any) -> Any:
            result = client._make_api_call(operation, params)
            return await result if args.asynchronous else result

        for client in pair.values():
            for _ in range(20):
                await call(client)
        count = 16
        while True:
            start = time.process_time()
            for _ in range(count):
                await call(pair["stock"])
            if time.process_time() - start > args.duration:
                break
            count *= 2
        for trial in range(args.trials):
            totals = dict.fromkeys(pair, 0.0)
            for block in range(8):
                for variant in order(trial + block):
                    start = time.process_time()
                    for _ in range(count // 8):
                        await call(pair[variant])
                    totals[variant] += time.process_time() - start
            for variant in order(trial):
                emit(
                    mode="cpu",
                    asynchronous=args.asynchronous,
                    variant=variant,
                    trial=trial,
                    workload=args.workload,
                    count=count,
                    cpu_us_op=totals[variant] / count * 1e6,
                )


async def network(args: argparse.Namespace) -> None:
    os.sched_setaffinity(
        0, {args.cpu if args.cpu is not None else min(os.sched_getaffinity(0))}
    )
    async with clients(args) as pair:
        operation, params, _ = WORKLOADS[args.workload]
        for client in pair.values():
            for _ in range(10):
                result = client._make_api_call(operation, params)
                if args.asynchronous:
                    await result
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.concurrency
        ) as pool:
            for trial in range(args.trials):
                for variant in order(trial):
                    client = pair[variant]
                    latencies = []
                    gate = threading.Barrier(args.concurrency + 1)

                    deadline = time.perf_counter() + args.duration

                    def sync_work(
                        client: Any = client,
                        gate: threading.Barrier = gate,
                        end: float = deadline,
                    ) -> list[float]:
                        gate.wait()
                        samples = []
                        while time.perf_counter() < end:
                            start = time.perf_counter()
                            client._make_api_call(operation, params)
                            samples.append(time.perf_counter() - start)
                        return samples

                    async def async_work(
                        client: Any = client, end: float = deadline
                    ) -> list[float]:
                        samples = []
                        while time.perf_counter() < end:
                            start = time.perf_counter()
                            await client._make_api_call(operation, params)
                            samples.append(time.perf_counter() - start)
                        return samples

                    before = cgroup_stat()
                    start, cpu_start = time.perf_counter(), time.process_time()
                    if args.asynchronous:
                        batches = await asyncio.gather(
                            *(async_work() for _ in range(args.concurrency))
                        )
                    else:
                        pending = [
                            pool.submit(sync_work) for _ in range(args.concurrency)
                        ]
                        gate.wait()
                        batches = [f.result() for f in pending]
                    latencies = sorted(v for batch in batches for v in batch)
                    elapsed, used = (
                        time.perf_counter() - start,
                        time.process_time() - cpu_start,
                    )
                    after = cgroup_stat()
                    emit(
                        mode="network",
                        asynchronous=args.asynchronous,
                        variant=variant,
                        trial=trial,
                        workload=args.workload,
                        concurrency=args.concurrency,
                        quota=args.quota,
                        count=len(latencies),
                        wall_s=elapsed,
                        rps=len(latencies) / elapsed,
                        cpu_us_op=used / len(latencies) * 1e6,
                        mean_ms=statistics.mean(latencies) * 1000,
                        p99_ms=latencies[int(0.99 * (len(latencies) - 1))] * 1000,
                        cgroup_delta={k: after[k] - v for k, v in before.items()},
                    )


async def startup(args: argparse.Namespace) -> None:
    # Incremental installation cost on the same already constructed client/model.
    async with clients(args) as pair:
        for trial in range(args.trials):
            client = pair["stock"]
            gc.collect()
            start = time.process_time()
            wall_start = time.perf_counter()
            install_response_parser(client, asynchronous=args.asynchronous)
            plain_elapsed = time.process_time() - start
            plain_wall = time.perf_counter() - wall_start
            client._endpoint._response_parser_factory = (
                client._endpoint._response_parser_factory._original
            )
            gc.collect()
            tracemalloc.start()
            start = time.process_time()
            install_response_parser(client, asynchronous=args.asynchronous)
            elapsed = time.process_time() - start
            retained, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            emit(
                mode="startup",
                asynchronous=args.asynchronous,
                trial=trial,
                cpu_ms=plain_elapsed * 1000,
                wall_ms=plain_wall * 1000,
                traced_cpu_ms=elapsed * 1000,
                retained_bytes=retained,
                peak_bytes=peak,
                note="warm service model; separate untraced timing and traced allocation runs",
            )
            client._endpoint._response_parser_factory = (
                client._endpoint._response_parser_factory._original
            )


async def server(args: argparse.Namespace) -> None:
    os.sched_setaffinity(0, {max(os.sched_getaffinity(0))})

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                headers = {
                    k.lower(): v.strip()
                    for k, v in (
                        line.split(b":", 1)
                        for line in head.split(b"\r\n")[1:]
                        if b":" in line
                    )
                }
                await reader.readexactly(int(headers.get(b"content-length", b"0")))
                if head.startswith(b"GET "):
                    body = b'["127.0.0.1"]'
                else:
                    # A header chooses the workload, allowing empty and nested
                    # GetItem to share the same server process and SDK operation.
                    name = headers[b"x-benchmark-workload"].decode()
                    body = BODIES[name]
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amzn-RequestId: bench\r\nContent-Length: "
                    + str(len(body)).encode()
                    + b"\r\n\r\n"
                    + body
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    peer = await asyncio.start_server(handle, "127.0.0.1", 0, limit=1024 * 1024)
    print(peer.sockets[0].getsockname()[1], flush=True)
    async with peer:
        await peer.serve_forever()


def run(args: argparse.Namespace) -> None:
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    environment = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_sha256": hashlib.sha256(
            pathlib.Path(__file__).read_bytes()
        ).hexdigest(),
        "sources": {
            str(
                path.relative_to(pathlib.Path(__file__).resolve().parents[1])
            ): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                pathlib.Path(__file__).resolve().parents[1] / "alternator"
            ).rglob("*.py")
        },
        "cpu_model": next(
            line.split(":", 1)[1].strip()
            for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "python": sys.version,
        "kernel": platform.release(),
        "machine": platform.machine(),
        "boto3": boto3.__version__,
        "botocore": botocore.__version__,
        "aiobotocore": aiobotocore.__version__,
        "affinity": sorted(os.sched_getaffinity(0)),
        "client_cpu": args.cpu
        if args.cpu is not None
        else min(os.sched_getaffinity(0)),
        "server_cpu": max(os.sched_getaffinity(0)),
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "patch": subprocess.check_output(
            ["git", "diff", "--", "alternator"], text=True
        ),
        "parameter_validation": True,
        "signed": False,
        "trials": args.trials,
        "network_duration": args.duration,
        "method": "alternating pairs; client pinned to client_cpu; server highest CPU outside quota",
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    script = str(pathlib.Path(__file__).resolve())
    peer = subprocess.Popen(
        [sys.executable, script, "server"], stdout=subprocess.PIPE, text=True
    )
    try:
        port = peer.stdout.readline().strip()
        for asynchronous in (False, True):
            kind = "async" if asynchronous else "sync"
            flags = ["--asynchronous"] if asynchronous else []
            if args.cpu is not None:
                flags += ["--cpu", str(args.cpu)]
            for mode in ("startup", "cpu", "network"):
                quotas = ("25%", "100%") if mode == "network" else ("none",)
                for quota in quotas:
                    concurrencies = (1, 8) if mode == "network" else (1,)
                    for concurrency in concurrencies:
                        path = output / f"{mode}-{kind}-{quota}-{concurrency}.jsonl"
                        with path.open("w") as raw:
                            names = args.workloads or WORKLOADS
                            if mode == "startup":
                                names = ["get_nested"]
                            for workload in names:
                                print(
                                    mode, kind, quota, concurrency, workload, flush=True
                                )
                                cmd = [
                                    sys.executable,
                                    script,
                                    mode,
                                    "--port",
                                    port,
                                    "--workload",
                                    workload,
                                    "--trials",
                                    str(args.trials),
                                    "--duration",
                                    str(args.duration if mode == "network" else 0.12),
                                    "--concurrency",
                                    str(concurrency),
                                    "--quota",
                                    quota,
                                    *flags,
                                ]
                                if mode == "network":
                                    cmd = [
                                        "systemd-run",
                                        "--user",
                                        "--scope",
                                        "--quiet",
                                        "-p",
                                        f"CPUQuota={quota}",
                                        *cmd,
                                    ]
                                subprocess.run(cmd, stdout=raw, check=True)
    finally:
        peer.terminate()
        peer.wait(timeout=10)


def summarize(args: argparse.Namespace) -> None:
    groups: dict[tuple[object, ...], list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(pathlib.Path(args.output).glob("*.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if row["mode"] == "startup":
                key = ("startup", row["asynchronous"])
            else:
                key = (
                    row["mode"],
                    row["asynchronous"],
                    row["workload"],
                    row.get("quota"),
                    row.get("concurrency"),
                )
            groups[key].append(row)
    summaries = []
    for key, rows in groups.items():
        result = {"group": key}
        if key[0] == "startup":
            for metric in ("cpu_ms", "wall_ms", "retained_bytes", "peak_bytes"):
                result[metric] = statistics.median(r[metric] for r in rows)
        else:
            pairs: dict[int, dict[str, Any]] = defaultdict(dict)
            for row in rows:
                pairs[row["trial"]][row["variant"]] = row
            result["incomplete_trials"] = [
                trial for trial, pair in pairs.items() if len(pair) != 2
            ]
            pairs = {trial: pair for trial, pair in pairs.items() if len(pair) == 2}
            if not pairs:
                result["status"] = "no_complete_pairs"
                summaries.append(result)
                continue
            for metric in ("cpu_us_op", "rps", "mean_ms", "p99_ms"):
                if metric not in rows[0]:
                    continue
                ratios = [
                    100 * (p["compiled"][metric] / p["stock"][metric] - 1)
                    for p in pairs.values()
                ]
                result[metric] = {
                    "stock": statistics.median(
                        p["stock"][metric] for p in pairs.values()
                    ),
                    "compiled": statistics.median(
                        p["compiled"][metric] for p in pairs.values()
                    ),
                    "paired_change_pct": statistics.median(ratios),
                    "paired_min_pct": min(ratios),
                    "paired_max_pct": max(ratios),
                    "pairs": len(ratios),
                }
        summaries.append(result)
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", choices=["run", "server", "cpu", "network", "startup", "summarize"]
    )
    parser.add_argument("--output", default="/tmp/response-parser-benchmark")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--workload", choices=WORKLOADS, default="get_nested")
    parser.add_argument("--workloads", nargs="+", choices=WORKLOADS)
    parser.add_argument("--asynchronous", action="store_true")
    parser.add_argument(
        "--cpu",
        type=int,
        help="Pin client to this CPU; server stays on highest allowed CPU",
    )
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--quota", default="none")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--duration", type=float, default=1.5)
    args = parser.parse_args()
    if args.mode in {"run", "summarize"}:
        globals()[args.mode](args)
    else:
        asyncio.run(globals()[args.mode](args))
