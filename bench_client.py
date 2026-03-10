#!/usr/bin/env python3
import argparse
import json
import os
import signal
import socket
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO


READ_MODES = ("ping", "get", "exists")
WRITE_MODES = ("set", "del")
ALL_MODES = READ_MODES + WRITE_MODES
SPAWN_HOST = "127.0.0.1"
SPAWN_PORT = 6380


@dataclass
class BenchmarkResult:
    fsync: str
    mode: str
    ops: int
    total_sec: float
    ops_per_sec: float
    avg_us: float

    def to_dict(self) -> dict[str, str | int | float]:
        return asdict(self)

    def to_text(self) -> str:
        return (
            f"fsync={self.fsync} mode={self.mode} ops={self.ops} "
            f"total_sec={self.total_sec:.6f} ops_per_sec={self.ops_per_sec:.2f} "
            f"avg_us={self.avg_us:.2f}"
        )


def read_resp(sock: socket.socket) -> bytes:
    data = b""

    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("server closed connection")
        data += chunk

        if data.startswith((b"+", b"-", b":")) and data.endswith(b"\r\n"):
            return data

        if data.startswith(b"$"):
            head_end = data.find(b"\r\n")
            if head_end != -1:
                length = int(data[1:head_end])
                if length == -1:
                    return data[: head_end + 2]
                total = head_end + 2 + length + 2
                if len(data) >= total:
                    return data[:total]


def encode_ping() -> bytes:
    return b"*1\r\n$4\r\nPING\r\n"


def encode_get(key: bytes) -> bytes:
    return (
        b"*2\r\n"
        + b"$3\r\nGET\r\n"
        + f"${len(key)}\r\n".encode()
        + key
        + b"\r\n"
    )


def encode_exists(key: bytes) -> bytes:
    return (
        b"*2\r\n"
        + b"$6\r\nEXISTS\r\n"
        + f"${len(key)}\r\n".encode()
        + key
        + b"\r\n"
    )


def encode_set(key: bytes, value: bytes) -> bytes:
    return (
        b"*3\r\n"
        + b"$3\r\nSET\r\n"
        + f"${len(key)}\r\n".encode()
        + key
        + b"\r\n"
        + f"${len(value)}\r\n".encode()
        + value
        + b"\r\n"
    )


def encode_del(key: bytes) -> bytes:
    return (
        b"*2\r\n"
        + b"$3\r\nDEL\r\n"
        + f"${len(key)}\r\n".encode()
        + key
        + b"\r\n"
    )


def port_is_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def ensure_port_available(host: str, port: int):
    if port_is_open(host, port):
        raise RuntimeError(
            f"refusing to spawn benchmark server: {host}:{port} is already accepting connections"
        )


def wait_for_server(
    host: str,
    port: int,
    timeout_sec: float,
    proc: subprocess.Popen | None = None,
):
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"server exited before startup completed (code={proc.returncode})")

        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)

    raise RuntimeError(f"server did not start on {host}:{port} within {timeout_sec}s")


def start_server(
    server_bin: str,
    fsync: str,
    host: str,
    port: int,
    wait_sec: float,
    server_cwd: str | None = None,
    server_stdout: int | BinaryIO | None = None,
    server_stderr: int | BinaryIO | None = None,
) -> subprocess.Popen:
    if host != SPAWN_HOST or port != SPAWN_PORT:
        raise RuntimeError(
            "spawned kvred benchmarks currently require "
            f"{SPAWN_HOST}:{SPAWN_PORT} because src/main.rs hardcodes that listen address"
        )

    ensure_port_available(host, port)

    env = os.environ.copy()
    env["KVRED_FSYNC"] = fsync
    stdout = subprocess.DEVNULL if server_stdout is None else server_stdout
    stderr = subprocess.DEVNULL if server_stderr is None else server_stderr

    proc = subprocess.Popen(
        [server_bin],
        env=env,
        cwd=server_cwd,
        stdout=stdout,
        stderr=stderr,
    )

    try:
        wait_for_server(host, port, wait_sec, proc=proc)
    except Exception:
        proc.kill()
        proc.wait()
        raise

    return proc


def stop_server(proc: subprocess.Popen, wait_sec: float):
    proc.send_signal(signal.SIGINT)

    try:
        proc.wait(timeout=wait_sec)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def prepare_mode(sock: socket.socket, mode: str, keyspace: int, ops: int, value: bytes):
    if mode in ("get", "exists"):
        sock.sendall(encode_set(b"bench", value))
        read_resp(sock)

    if mode == "del":
        for i in range(ops):
            key = f"k{i}".encode()
            sock.sendall(encode_set(key, value))
            read_resp(sock)


def build_request(mode: str, index: int, keyspace: int, value: bytes) -> bytes:
    if mode == "ping":
        return encode_ping()

    if mode == "get":
        return encode_get(b"bench")

    if mode == "exists":
        return encode_exists(b"bench")

    if mode == "set":
        key = f"k{index % keyspace}".encode()
        return encode_set(key, value)

    if mode == "del":
        key = f"k{index}".encode()
        return encode_del(key)

    raise ValueError(f"unsupported benchmark mode: {mode}")


def run_benchmark(
    *,
    host: str,
    port: int,
    mode: str,
    ops: int,
    keyspace: int,
    value: bytes,
    fsync: str,
    spawn_server: bool = False,
    server_bin: str = "./target/release/kvred",
    startup_wait: float = 2.0,
    shutdown_wait: float = 5.0,
    server_cwd: str | None = None,
    server_stdout: int | BinaryIO | None = None,
    server_stderr: int | BinaryIO | None = None,
) -> BenchmarkResult:
    proc = None

    try:
        if spawn_server:
            proc = start_server(
                server_bin=server_bin,
                fsync=fsync,
                host=host,
                port=port,
                wait_sec=startup_wait,
                server_cwd=server_cwd,
                server_stdout=server_stdout,
                server_stderr=server_stderr,
            )

        with socket.create_connection((host, port)) as sock:
            prepare_mode(sock, mode, keyspace, ops, value)

            start = time.perf_counter()

            for i in range(ops):
                sock.sendall(build_request(mode, i, keyspace, value))
                read_resp(sock)

            end = time.perf_counter()

        total = end - start
        return BenchmarkResult(
            fsync=fsync,
            mode=mode,
            ops=ops,
            total_sec=total,
            ops_per_sec=ops / total,
            avg_us=(total * 1_000_000) / ops,
        )

    finally:
        if proc is not None:
            stop_server(proc, shutdown_wait)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--mode", choices=ALL_MODES, required=True)
    parser.add_argument("-n", "--ops", type=int, default=10000)
    parser.add_argument("--keyspace", type=int, default=1000)
    parser.add_argument(
        "--value-size",
        type=int,
        default=5,
        help="Value size in bytes for write benchmarks and GET/EXISTS priming.",
    )
    parser.add_argument(
        "--fsync",
        choices=["always", "everysec", "none"],
        default="always",
        help="Label for benchmark output, and server mode if --spawn-server is used.",
    )
    parser.add_argument(
        "--spawn-server",
        action="store_true",
        help="Spawn kvred for this benchmark run using --fsync.",
    )
    parser.add_argument(
        "--server-bin",
        default="./target/release/kvred",
        help="Path to kvred binary when using --spawn-server.",
    )
    parser.add_argument(
        "--server-cwd",
        default=None,
        help="Working directory for spawned kvred process.",
    )
    parser.add_argument(
        "--startup-wait",
        type=float,
        default=2.0,
        help="Seconds to wait for spawned server startup.",
    )
    parser.add_argument(
        "--shutdown-wait",
        type=float,
        default=5.0,
        help="Seconds to wait for graceful server shutdown.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format for the benchmark result.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    server_bin = str(Path(args.server_bin).resolve())
    value = b"v" * args.value_size

    result = run_benchmark(
        host=args.host,
        port=args.port,
        mode=args.mode,
        ops=args.ops,
        keyspace=args.keyspace,
        value=value,
        fsync=args.fsync,
        spawn_server=args.spawn_server,
        server_bin=server_bin,
        startup_wait=args.startup_wait,
        shutdown_wait=args.shutdown_wait,
        server_cwd=args.server_cwd,
    )

    if args.format == "json":
        print(json.dumps(result.to_dict(), sort_keys=True))
    else:
        print(result.to_text())


if __name__ == "__main__":
    main()
