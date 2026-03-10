#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
import sys
import time
from html import escape
from pathlib import Path

from bench_client import (
    ALL_MODES,
    READ_MODES,
    run_benchmark,
    start_server,
    stop_server,
)


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6380
FSYNC_MODES = ("always", "everysec", "none")
MODE_ORDER = ("ping", "get", "exists", "set", "del")
PALETTE = {
    "ping": "#2563eb",
    "get": "#3b82f6",
    "exists": "#60a5fa",
    "set": "#ea580c",
    "del": "#dc2626",
}


def log(message: str):
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--server-bin",
        default="./target/release/kvred",
        help="Path to kvred binary.",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmark_reports",
        help="Directory where the timestamped benchmark report will be written.",
    )
    parser.add_argument(
        "--fsync",
        nargs="+",
        choices=FSYNC_MODES,
        default=list(FSYNC_MODES),
        help="Fsync policies to benchmark.",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=ALL_MODES,
        default=list(MODE_ORDER),
        help="Command modes to benchmark.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Measured runs per command and fsync mode.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup runs per command before measured repeats.",
    )
    parser.add_argument(
        "--ops-read",
        type=int,
        default=100000,
        help="Operations per measured read benchmark run.",
    )
    parser.add_argument(
        "--ops-write",
        type=int,
        default=10000,
        help="Operations per measured write benchmark run.",
    )
    parser.add_argument("--keyspace", type=int, default=1000)
    parser.add_argument(
        "--value-size",
        type=int,
        default=5,
        help="Value size in bytes for SET and priming.",
    )
    parser.add_argument("--startup-wait", type=float, default=2.0)
    parser.add_argument("--shutdown-wait", type=float, default=5.0)
    parser.add_argument(
        "--title",
        default="kvred benchmark report",
        help="Title prefix used in generated charts and markdown.",
    )
    return parser.parse_args()


def mode_group(mode: str) -> str:
    if mode in READ_MODES:
        return "read"
    return "write"


def ops_for_mode(args: argparse.Namespace, mode: str) -> int:
    if mode in READ_MODES:
        return args.ops_read
    return args.ops_write


def warmup_ops_for_mode(args: argparse.Namespace, mode: str) -> int:
    return min(1000, ops_for_mode(args, mode))


def format_ops(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1000:
        return f"{value / 1000:.1f}k"
    return f"{value:.0f}"


def format_us(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.2f} ms"
    return f"{value:.2f} us"


def render_svg_chart(
    path: Path,
    *,
    title: str,
    fsync: str,
    generated_at: str,
    summary_rows: list[dict[str, str | int | float]],
    repeats: int,
    ops_read: int,
    ops_write: int,
):
    width = 1320
    height = 760
    background = "#f5efe6"
    ink = "#18181b"
    panel_bg = "#fffdfa"
    grid = "#d6d3d1"

    read_rows = [row for row in summary_rows if row["group"] == "read"]
    write_rows = [row for row in summary_rows if row["group"] == "write"]

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{background}" />',
        '<style>',
        ".title { font: 700 34px 'Segoe UI', Arial, sans-serif; fill: #18181b; }",
        ".subtitle { font: 500 14px 'Segoe UI', Arial, sans-serif; fill: #57534e; }",
        ".panel-title { font: 700 20px 'Segoe UI', Arial, sans-serif; fill: #18181b; }",
        ".axis { font: 500 12px 'Segoe UI', Arial, sans-serif; fill: #57534e; }",
        ".bar-label { font: 700 13px 'Segoe UI', Arial, sans-serif; fill: #18181b; }",
        ".bar-note { font: 500 12px 'Segoe UI', Arial, sans-serif; fill: #57534e; }",
        ".footer { font: 500 12px 'Segoe UI', Arial, sans-serif; fill: #44403c; }",
        "</style>",
        f'<text x="60" y="70" class="title">{escape(title)} | fsync={escape(fsync)}</text>',
        (
            f'<text x="60" y="102" class="subtitle">'
            f'median ops/sec per command, min-max whiskers, single persistent TCP connection, '
            f'{repeats} measured run(s)</text>'
        ),
        (
            f'<text x="60" y="126" class="subtitle">'
            f'generated {escape(generated_at)} | read ops={ops_read} | write ops={ops_write}</text>'
        ),
    ]

    svg.extend(
        render_panel(
            x=50,
            y=160,
            width=610,
            height=500,
            title="Read Path",
            rows=read_rows,
            fill="#dbeafe",
            panel_bg=panel_bg,
            ink=ink,
            grid=grid,
        )
    )
    svg.extend(
        render_panel(
            x=690,
            y=160,
            width=580,
            height=500,
            title="Write Path",
            rows=write_rows,
            fill="#ffedd5",
            panel_bg=panel_bg,
            ink=ink,
            grid=grid,
        )
    )

    svg.append(
        '<text x="60" y="710" class="footer">Reads bypass the write sequencer. Writes go through AOF append and fsync policy. Separate panels keep both scales legible.</text>'
    )
    svg.append("</svg>")
    path.write_text("".join(svg), encoding="utf-8")


def render_panel(
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    rows: list[dict[str, str | int | float]],
    fill: str,
    panel_bg: str,
    ink: str,
    grid: str,
) -> list[str]:
    svg = [
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="24" fill="{panel_bg}" stroke="#e7e5e4" />',
        f'<rect x="{x}" y="{y}" width="{width}" height="72" rx="24" fill="{fill}" />',
        f'<text x="{x + 28}" y="{y + 44}" class="panel-title">{escape(title)}</text>',
    ]

    if not rows:
        svg.append(
            f'<text x="{x + 28}" y="{y + 130}" class="axis">no benchmark data for this panel</text>'
        )
        return svg

    plot_x = x + 60
    plot_y = y + 108
    plot_w = width - 100
    plot_h = height - 190
    tick_count = 4
    max_value = max(float(row["ops_per_sec_max"]) for row in rows)
    max_value *= 1.15

    for tick in range(tick_count + 1):
        value = max_value * tick / tick_count
        y_pos = plot_y + plot_h - (value / max_value) * plot_h if max_value else plot_y + plot_h
        svg.append(
            f'<line x1="{plot_x}" y1="{y_pos:.1f}" x2="{plot_x + plot_w}" y2="{y_pos:.1f}" stroke="{grid}" stroke-dasharray="4 8" />'
        )
        svg.append(
            f'<text x="{plot_x - 12}" y="{y_pos + 4:.1f}" text-anchor="end" class="axis">{format_ops(value)}</text>'
        )

    bar_gap = 28
    bar_width = (plot_w - bar_gap * (len(rows) + 1)) / len(rows)

    for index, row in enumerate(rows):
        mode = str(row["mode"])
        x_pos = plot_x + bar_gap + index * (bar_width + bar_gap)
        median = float(row["ops_per_sec_median"])
        min_value = float(row["ops_per_sec_min"])
        max_row_value = float(row["ops_per_sec_max"])
        avg_us = float(row["avg_us_median"])

        bar_height = (median / max_value) * plot_h if max_value else 0
        bar_top = plot_y + plot_h - bar_height

        min_y = plot_y + plot_h - (min_value / max_value) * plot_h if max_value else plot_y + plot_h
        max_y = plot_y + plot_h - (max_row_value / max_value) * plot_h if max_value else plot_y + plot_h
        center_x = x_pos + bar_width / 2

        svg.append(
            f'<line x1="{center_x:.1f}" y1="{max_y:.1f}" x2="{center_x:.1f}" y2="{min_y:.1f}" stroke="{ink}" stroke-width="2" />'
        )
        svg.append(
            f'<line x1="{center_x - 9:.1f}" y1="{max_y:.1f}" x2="{center_x + 9:.1f}" y2="{max_y:.1f}" stroke="{ink}" stroke-width="2" />'
        )
        svg.append(
            f'<line x1="{center_x - 9:.1f}" y1="{min_y:.1f}" x2="{center_x + 9:.1f}" y2="{min_y:.1f}" stroke="{ink}" stroke-width="2" />'
        )
        svg.append(
            f'<rect x="{x_pos:.1f}" y="{bar_top:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" rx="18" fill="{PALETTE[mode]}" />'
        )
        svg.append(
            f'<text x="{center_x:.1f}" y="{bar_top - 16:.1f}" text-anchor="middle" class="bar-label">{format_ops(median)} ops/s</text>'
        )
        svg.append(
            f'<text x="{center_x:.1f}" y="{plot_y + plot_h + 34}" text-anchor="middle" class="bar-label">{escape(mode.upper())}</text>'
        )
        svg.append(
            f'<text x="{center_x:.1f}" y="{plot_y + plot_h + 54}" text-anchor="middle" class="bar-note">{format_us(avg_us)}</text>'
        )

    svg.append(
        f'<text x="{plot_x}" y="{plot_y + plot_h + 88}" class="axis">bar label = median throughput | below bar = median latency</text>'
    )
    return svg


def summarize_results(
    rows: list[dict[str, str | int | float]],
    fsync_modes: list[str],
    selected_modes: list[str],
) -> list[dict[str, str | int | float]]:
    summary = []

    for fsync in fsync_modes:
        for mode in MODE_ORDER:
            if mode not in selected_modes:
                continue

            mode_rows = [
                row for row in rows if row["fsync"] == fsync and row["mode"] == mode and row["phase"] == "measured"
            ]
            if not mode_rows:
                continue

            ops_values = [float(row["ops_per_sec"]) for row in mode_rows]
            avg_us_values = [float(row["avg_us"]) for row in mode_rows]
            total_sec_values = [float(row["total_sec"]) for row in mode_rows]

            summary.append(
                {
                    "fsync": fsync,
                    "mode": mode,
                    "group": mode_group(mode),
                    "ops": int(mode_rows[0]["ops"]),
                    "repeats": len(mode_rows),
                    "ops_per_sec_median": statistics.median(ops_values),
                    "ops_per_sec_min": min(ops_values),
                    "ops_per_sec_max": max(ops_values),
                    "avg_us_median": statistics.median(avg_us_values),
                    "avg_us_min": min(avg_us_values),
                    "avg_us_max": max(avg_us_values),
                    "total_sec_median": statistics.median(total_sec_values),
                }
            )

    return summary


def write_csv(path: Path, rows: list[dict[str, str | int | float]]):
    if not rows:
        return

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    *,
    title: str,
    generated_at: str,
    fsync_modes: list[str],
    summary_rows: list[dict[str, str | int | float]],
    repeats: int,
    ops_read: int,
    ops_write: int,
):
    lines = [
        f"# {title}",
        "",
        f"- Generated: {generated_at}",
        f"- Measured repeats: {repeats}",
        f"- Read ops per run: {ops_read}",
        f"- Write ops per run: {ops_write}",
        "",
    ]

    for fsync in fsync_modes:
        lines.append(f"## fsync={fsync}")
        lines.append("")
        lines.append(f"![fsync={fsync}](charts/fsync-{fsync}.svg)")
        lines.append("")
        lines.append("| Command | Ops | Median ops/sec | Median latency | Range ops/sec |")
        lines.append("|---|---:|---:|---:|---:|")

        for row in summary_rows:
            if row["fsync"] != fsync:
                continue
            lines.append(
                f"| `{str(row['mode']).upper()}` | {int(row['ops'])} | "
                f"{float(row['ops_per_sec_median']):.2f} | {float(row['avg_us_median']):.2f} us | "
                f"{float(row['ops_per_sec_min']):.2f} - {float(row['ops_per_sec_max']):.2f} |"
            )

        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def expected_stage_count(args: argparse.Namespace) -> int:
    return len(args.fsync) * len(args.modes) * (args.warmup + args.repeats)


def persist_report(
    *,
    args: argparse.Namespace,
    output_root: Path,
    raw_dir: Path,
    charts_dir: Path,
    generated_at: str,
    server_bin: str,
    raw_rows: list[dict[str, str | int | float]],
    interrupted: bool,
    current_step: int,
    total_steps: int,
):
    summary_rows = summarize_results(raw_rows, args.fsync, args.modes)

    for fsync in args.fsync:
        fsync_rows = [row for row in summary_rows if row["fsync"] == fsync]
        render_svg_chart(
            charts_dir / f"fsync-{fsync}.svg",
            title=args.title,
            fsync=fsync,
            generated_at=generated_at,
            summary_rows=fsync_rows,
            repeats=args.repeats,
            ops_read=args.ops_read,
            ops_write=args.ops_write,
        )

    metadata = {
        "title": args.title,
        "generated_at": generated_at,
        "server_bin": server_bin,
        "fsync": args.fsync,
        "modes": args.modes,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "ops_read": args.ops_read,
        "ops_write": args.ops_write,
        "keyspace": args.keyspace,
        "value_size": args.value_size,
        "interrupted": interrupted,
        "completed_steps": current_step,
        "total_steps": total_steps,
    }

    (raw_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (raw_dir / "results.json").write_text(json.dumps(raw_rows, indent=2), encoding="utf-8")
    (raw_dir / "summary.json").write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")
    write_csv(raw_dir / "results.csv", raw_rows)
    write_csv(raw_dir / "summary.csv", summary_rows)
    write_markdown(
        output_root / "summary.md",
        title=args.title,
        generated_at=generated_at,
        fsync_modes=args.fsync,
        summary_rows=summary_rows,
        repeats=args.repeats,
        ops_read=args.ops_read,
        ops_write=args.ops_write,
    )


def warn_if_slow(args: argparse.Namespace):
    if "always" in args.fsync and any(mode not in READ_MODES for mode in args.modes):
        if args.ops_write >= 10_000:
            log(
                "warning: fsync=always with --ops-write >= 10000 can take several minutes "
                "because every write and DEL prefill hits fsync"
            )


def main():
    args = parse_args()
    server_bin = str(Path(args.server_bin).resolve())
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_root = Path(args.output_dir) / timestamp
    charts_dir = output_root / "charts"
    raw_dir = output_root / "raw"
    charts_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    generated_at = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    value = b"v" * args.value_size
    raw_rows: list[dict[str, str | int | float]] = []
    total_steps = expected_stage_count(args)
    current_step = 0
    interrupted = False

    log(f"report dir: {output_root}")
    warn_if_slow(args)
    persist_report(
        args=args,
        output_root=output_root,
        raw_dir=raw_dir,
        charts_dir=charts_dir,
        generated_at=generated_at,
        server_bin=server_bin,
        raw_rows=raw_rows,
        interrupted=interrupted,
        current_step=current_step,
        total_steps=total_steps,
    )

    try:
        for fsync in args.fsync:
            log(f"starting fsync={fsync}")
            server_dir = output_root / "servers" / fsync
            server_dir.mkdir(parents=True, exist_ok=True)
            stdout_path = server_dir / "stdout.log"
            stderr_path = server_dir / "stderr.log"

            with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
                "w", encoding="utf-8"
            ) as stderr_handle:
                proc = start_server(
                    server_bin=server_bin,
                    fsync=fsync,
                    host=args.host,
                    port=args.port,
                    wait_sec=args.startup_wait,
                    server_cwd=str(server_dir),
                    server_stdout=stdout_handle,
                    server_stderr=stderr_handle,
                )

                try:
                    for mode in MODE_ORDER:
                        if mode not in args.modes:
                            continue

                        for warmup_index in range(args.warmup):
                            step_number = current_step + 1
                            ops = warmup_ops_for_mode(args, mode)
                            log(
                                f"[{step_number}/{total_steps}] warmup fsync={fsync} "
                                f"mode={mode} run={warmup_index + 1}/{args.warmup} ops={ops}"
                            )
                            result = run_benchmark(
                                host=args.host,
                                port=args.port,
                                mode=mode,
                                ops=ops,
                                keyspace=args.keyspace,
                                value=value,
                                fsync=fsync,
                            )
                            raw_rows.append(
                                result.to_dict()
                                | {
                                    "phase": "warmup",
                                    "run_index": warmup_index,
                                }
                            )
                            current_step = step_number
                            persist_report(
                                args=args,
                                output_root=output_root,
                                raw_dir=raw_dir,
                                charts_dir=charts_dir,
                                generated_at=generated_at,
                                server_bin=server_bin,
                                raw_rows=raw_rows,
                                interrupted=interrupted,
                                current_step=current_step,
                                total_steps=total_steps,
                            )

                        for repeat_index in range(args.repeats):
                            step_number = current_step + 1
                            ops = ops_for_mode(args, mode)
                            log(
                                f"[{step_number}/{total_steps}] measured fsync={fsync} "
                                f"mode={mode} run={repeat_index + 1}/{args.repeats} ops={ops}"
                            )
                            result = run_benchmark(
                                host=args.host,
                                port=args.port,
                                mode=mode,
                                ops=ops,
                                keyspace=args.keyspace,
                                value=value,
                                fsync=fsync,
                            )
                            raw_rows.append(
                                result.to_dict()
                                | {
                                    "phase": "measured",
                                    "run_index": repeat_index,
                                }
                            )
                            current_step = step_number
                            log(
                                f"completed fsync={fsync} mode={mode} "
                                f"run={repeat_index + 1}/{args.repeats} "
                                f"ops_per_sec={result.ops_per_sec:.2f} avg_us={result.avg_us:.2f}"
                            )
                            persist_report(
                                args=args,
                                output_root=output_root,
                                raw_dir=raw_dir,
                                charts_dir=charts_dir,
                                generated_at=generated_at,
                                server_bin=server_bin,
                                raw_rows=raw_rows,
                                interrupted=interrupted,
                                current_step=current_step,
                                total_steps=total_steps,
                            )
                finally:
                    stop_server(proc, args.shutdown_wait)
    except KeyboardInterrupt:
        interrupted = True
        log("benchmark interrupted; writing partial report")
    finally:
        persist_report(
            args=args,
            output_root=output_root,
            raw_dir=raw_dir,
            charts_dir=charts_dir,
            generated_at=generated_at,
            server_bin=server_bin,
            raw_rows=raw_rows,
            interrupted=interrupted,
            current_step=current_step,
            total_steps=total_steps,
        )

    log(str(output_root))
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    main()
