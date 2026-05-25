"""Analysis module for horizontal scaling / resharding tests."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.chart_markers import mark_test_window
from src.failover import parse_time_series

matplotlib.use("Agg")


def _load_timing(path: Path) -> Dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _reshard_files(json_dir: Path, pattern: str) -> List[Path]:
    files = list(json_dir.glob(pattern))
    files.extend(p for p in json_dir.rglob(pattern) if p.parent != json_dir)
    return files


def _discover_reshard_files(json_dir: Path) -> List[Tuple[str, str, Optional[str], Path, Path]]:
    discovered: List[Tuple[str, str, Optional[str], Path, Path]] = []

    for f in _reshard_files(json_dir, "reshard_run_*.json"):
        run_num = f.stem.split("_")[-1]
        discovered.append(("up", run_num, None, f, f.parent / f"reshard_timing_{run_num}.json"))

    for f in _reshard_files(json_dir, "reshard_up_run_*.json"):
        run_num = f.stem.split("_")[-1]
        timing_path = f.parent / f"reshard_up_timing_{run_num}.json"
        if not timing_path.exists():
            timing_path = f.parent / f"reshard_timing_{run_num}.json"
        discovered.append(("up", run_num, None, f, timing_path))

    for f in _reshard_files(json_dir, "reshard_down_run_*.json"):
        run_num = f.stem.split("_")[-1]
        discovered.append(("down", run_num, None, f, f.parent / f"reshard_down_timing_{run_num}.json"))

    for f in _reshard_files(json_dir, "reshard_*_run_*.json"):
        parts = f.stem.split("_")
        if len(parts) == 4 and parts[0] == "reshard" and parts[2] == "run":
            mode = parts[1]
            if mode in {"up", "down"}:
                continue
            run_num = parts[3]
            discovered.append(("up", run_num, mode, f, f.parent / f"reshard_{mode}_timing_{run_num}.json"))
        elif len(parts) == 5 and parts[0] == "reshard" and parts[2] == "down" and parts[3] == "run":
            mode = parts[1]
            run_num = parts[4]
            discovered.append(
                ("down", run_num, mode, f, f.parent / f"reshard_{mode}_down_timing_{run_num}.json")
            )

    phase_order = {"up": 0, "down": 1}
    mode_order = {None: 0, "legacy": 1, "atomic": 2, "ms": 3}
    return sorted(
        discovered,
        key=lambda item: (
            int(item[1]) if item[1].isdigit() else item[1],
            mode_order.get(item[2], 99),
            str(item[2] or ""),
            phase_order[item[0]],
        ),
    )


def analyse_reshard_runs(
    json_dir: Path,
) -> Tuple[pd.DataFrame, List[pd.DataFrame], List[List[Dict]], List[Dict]]:
    """Parse reshard memtier and timing files.

    Returns (summary_df, list_of_timeseries, list_of_windows, list_of_timings).
    """
    discovered_files = _discover_reshard_files(json_dir)
    if not discovered_files:
        raise FileNotFoundError(f"No reshard memtier JSON files in {json_dir}")

    rows = []
    all_ts: List[pd.DataFrame] = []
    all_windows: List[List[Dict]] = []
    all_timings: List[Dict] = []

    for phase, run_num, mode, f, timing_path in discovered_files:
        with f.open() as fh:
            doc = json.load(fh)

        run_result = doc.get("RUN #1 RESULTS") or doc.get("ALL STATS")
        if run_result is None:
            for key in doc:
                if key.startswith("RUN #") and key.endswith("RESULTS"):
                    run_result = doc[key]
                    break

        if run_result is None:
            print(f"  [warn] No run results found in {f.name}, skipping")
            continue

        ts = parse_time_series(run_result)
        all_ts.append(ts)
        all_windows.append([])

        timing = {}
        if timing_path.exists():
            timing = _load_timing(timing_path)
        all_timings.append(timing)

        provider = timing.get("provider")
        migration_mode = timing.get("slot_migration_mode", mode)
        if _is_memorystore_provider(provider):
            migration_mode = "ms"

        explicit_rebalance_duration = timing.get("explicit_rebalance_duration_s")
        fallback_rebalance_duration = timing.get("fallback_rebalance_duration_s")
        fallback_rebalance_used = timing.get("fallback_rebalance_used") is True
        auto_rebalance_duration = (
            timing.get("auto_rebalance_duration_s")
            if timing.get("auto_rebalance_duration_s") is not None
            else timing.get("rebalance_duration_s")
        )
        if explicit_rebalance_duration is not None:
            slot_move_duration = explicit_rebalance_duration
        elif fallback_rebalance_used or fallback_rebalance_duration not in (None, 0):
            slot_move_duration = fallback_rebalance_duration
        else:
            slot_move_duration = auto_rebalance_duration

        operation_duration = (
            timing.get("operation_duration_s")
            or explicit_rebalance_duration
            or auto_rebalance_duration
            or timing.get("reshard_down_duration_s")
        )
        managed_blackbox_duration = operation_duration if migration_mode == "ms" else None

        row = {
            "file": _display_path(json_dir, f),
            "run": int(run_num) if run_num.isdigit() else run_num,
            "phase": timing.get("phase", phase),
            "provider": provider,
            "slot_migration_mode": migration_mode,
            "atomic_slot_migration": timing.get("atomic_slot_migration"),
            "slot_migration_command": timing.get("slot_migration_command"),
            "operation_status": timing.get("operation_status"),
            "operation_duration_s": operation_duration,
            "managed_blackbox_duration_s": managed_blackbox_duration,
            "scale_duration_s": None if migration_mode == "ms" else timing.get("scale_duration_s"),
            "rebalance_duration_s": auto_rebalance_duration,
            "explicit_rebalance_duration_s": explicit_rebalance_duration,
            "slot_move_duration_s": None if migration_mode == "ms" else slot_move_duration,
            "reshard_down_duration_s": None if migration_mode == "ms" else timing.get("reshard_down_duration_s"),
            "del_node_duration_s": timing.get("del_node_duration_s"),
            "scale_down_duration_s": None if migration_mode == "ms" else timing.get("scale_down_duration_s"),
            "auto_rebalance_detected": timing.get("auto_rebalance_detected"),
            "auto_rebalance_status": timing.get("auto_rebalance_status"),
            "slots_on_new_after": timing.get("slots_on_new_after"),
            "expected_slots_on_new": timing.get("expected_slots_on_new"),
            "auto_rebalance_trace": timing.get("auto_rebalance_trace"),
            "fallback_rebalance_used": timing.get("fallback_rebalance_used"),
            "fallback_rebalance_status": timing.get("fallback_rebalance_status"),
            "fallback_rebalance_duration_s": fallback_rebalance_duration,
            "fallback_rebalance_slots_before": timing.get("fallback_rebalance_slots_before"),
            "fallback_rebalance_slots_after": timing.get("fallback_rebalance_slots_after"),
            "explicit_rebalance_status": timing.get("explicit_rebalance_status"),
            "explicit_rebalance_slots_before": timing.get("explicit_rebalance_slots_before"),
            "explicit_rebalance_slots_after": timing.get("explicit_rebalance_slots_after"),
        }
        row["wait_check_duration_s"] = _wait_check_duration(row)
        rows.append(row)

    df = pd.DataFrame(rows)
    return df, all_ts, all_windows, all_timings


def _duration_number(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _is_memorystore_provider(provider: Any) -> bool:
    return isinstance(provider, str) and provider.startswith("memorystore_")


def _display_path(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _phase_component_fields(phase: str) -> List[str]:
    if phase == "down":
        return ["reshard_down_duration_s", "del_node_duration_s", "scale_down_duration_s"]
    return ["scale_duration_s", "slot_move_duration_s"]


def _wait_check_duration(row: Dict[str, Any]) -> Optional[float]:
    total = _duration_number(row.get("operation_duration_s"))
    if total is None:
        return None
    managed_total = _duration_number(row.get("managed_blackbox_duration_s"))
    if managed_total is not None:
        return max(0.0, total - managed_total)

    component_sum = 0.0
    for field in _phase_component_fields(str(row.get("phase", "up"))):
        value = _duration_number(row.get(field))
        if value is not None:
            component_sum += value

    return max(0.0, total - component_sum)


def print_reshard_summary(df: pd.DataFrame) -> None:
    valid = df

    print(f"\nReshard runs analysed: {len(df)}")
    if valid.empty:
        print("No valid data found.")
        return

    print("\nReshard operation duration per run")
    print(
        f"{'Phase':<8} {'Mode':<8} {'Run':>8} {'Total':>8} {'BlackBox':>9} {'Scale':>8} "
        f"{'Move':>8} {'Del':>8} {'ScaleDn':>8} {'Wait':>8} "
        f"{'Status':>12} {'Slots':>11}"
    )
    print("-" * 122)
    for _, row in valid.iterrows():
        slots = "-"
        if pd.notna(row.get("slots_on_new_after")) and pd.notna(row.get("expected_slots_on_new")):
            slots = f"{int(row['slots_on_new_after'])}/{int(row['expected_slots_on_new'])}"
        status = row.get("explicit_rebalance_status") or row.get("auto_rebalance_status")
        move_duration = (
            row.get("reshard_down_duration_s")
            if row.get("phase") == "down"
            else row.get("slot_move_duration_s")
        )
        print(
            f"{str(row.get('phase', '')):<8} "
            f"{_text_or_dash(row.get('slot_migration_mode')):<8} "
            f"{str(row.get('run', '')):>8} "
            f"{_duration_text(row.get('operation_duration_s')):>8} "
            f"{_duration_text(row.get('managed_blackbox_duration_s')):>9} "
            f"{_duration_text(row.get('scale_duration_s')):>8} "
            f"{_duration_text(move_duration):>8} "
            f"{_duration_text(row.get('del_node_duration_s')):>8} "
            f"{_duration_text(row.get('scale_down_duration_s')):>8} "
            f"{_duration_text(row.get('wait_check_duration_s')):>8} "
            f"{_text_or_dash(status):>12} "
            f"{slots:>11}"
        )


def _duration_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.0f}s"


def _text_or_dash(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return str(value)


def save_reshard_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "reshard_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nReshard CSV saved to {csv_path}")


def plot_reshard_timeseries(
    all_ts: List[pd.DataFrame],
    all_windows: List[List[Dict]],
    all_timings: List[Dict],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot ops/sec and latency over time, highlighting reshard operation windows."""
    for i, (ts, windows, timing, (_, row)) in enumerate(
        zip(all_ts, all_windows, all_timings, results_df.iterrows())
    ):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.6, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")

        phase = row.get("phase", timing.get("phase", "up"))
        mode = row.get("slot_migration_mode") or timing.get("slot_migration_mode")
        op_dur = timing.get("operation_duration_s")
        if op_dur is None:
            op_dur = timing.get("rebalance_duration_s", timing.get("reshard_down_duration_s", "?"))
        mode_text = f" {mode}" if mode else ""
        ax1.set_title(f"Reshard {phase}{mode_text} run {row.get('run', i + 1)} (operation: {op_dur}s)")

        test_window = _event_window(timing, windows)
        marked_phases = _mark_reshard_phases(ax1, timing, phase, with_labels=True)
        if test_window is not None:
            mark_test_window(
                ax1,
                test_window[0],
                test_window[1],
                with_labels=True,
                start_label="Reshard start",
                end_label="Reshard end",
            )
        if test_window is not None or marked_phases:
            ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.6, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.6, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_xlabel("Time (s)")
        ax2.legend(fontsize=8)

        _mark_reshard_phases(ax2, timing, phase, with_labels=False)
        if test_window is not None:
            mark_test_window(
                ax2,
                test_window[0],
                test_window[1],
                start_label="Reshard start",
                end_label="Reshard end",
            )

        fig.tight_layout()
        fig.savefig(out_dir / f"{_plot_stem(row['file'])}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def _timing_float(timing: Dict, key: str) -> Optional[float]:
    value = timing.get(key)
    if value is None:
        return None
    return float(value)


def _mark_reshard_phases(
    ax: matplotlib.axes.Axes,
    timing: Dict,
    phase: str,
    *,
    with_labels: bool = False,
) -> bool:
    if phase == "down":
        phase_windows = [
            ("Move slots off", "reshard_down_start_s", "reshard_down_end_s", "#8172b2"),
            ("Remove nodes", "del_node_start_s", "del_node_end_s", "#ccb974"),
            ("Scale down", "scale_down_start_s", "scale_down_end_s", "#64b5cd"),
        ]
    elif timing.get("explicit_rebalance_start_s") is not None:
        phase_windows = [
            ("Scale up", "scale_start_s", "scale_end_s", "#64b5cd"),
            ("Move slots", "explicit_rebalance_start_s", "explicit_rebalance_end_s", "#dd8452"),
        ]
    else:
        phase_windows = [
            ("Scale up", "scale_start_s", "scale_end_s", "#64b5cd"),
            ("Auto rebalance", "rebalance_start_s", "rebalance_end_s", "#8172b2"),
            ("Fallback rebalance", "fallback_rebalance_start_s", "fallback_rebalance_end_s", "#dd8452"),
        ]

    marked = False
    for label, start_key, end_key, color in phase_windows:
        start = _timing_float(timing, start_key)
        end = _timing_float(timing, end_key)
        if start is None or end is None:
            continue
        ax.axvline(
            start,
            linestyle=":",
            color=color,
            linewidth=1.2,
            alpha=0.9,
            label=label if with_labels else None,
        )
        if end != start:
            ax.axvline(
                end,
                linestyle="--",
                color=color,
                linewidth=1.0,
                alpha=0.9,
            )
        marked = True

    return marked


def _event_window(timing: Dict, windows: List[Dict]) -> Optional[Tuple[float, float]]:
    start = timing.get("operation_start_s")
    if start is None:
        start = timing.get("scale_start_s")
    if start is None:
        start = timing.get("rebalance_start_s")
    if start is None:
        start = timing.get("explicit_rebalance_start_s")
    if start is None:
        start = timing.get("reshard_down_start_s")

    end = timing.get("operation_end_s")
    if end is None:
        end = timing.get("rebalance_end_s")
    if end is None:
        end = timing.get("explicit_rebalance_end_s")
    if end is None:
        end = timing.get("scale_end_s")
    if end is None:
        end = timing.get("scale_down_end_s")
    if end is None:
        end = timing.get("reshard_down_end_s")

    if start is not None:
        return float(start), float(end if end is not None else start)

    if not windows:
        return None

    return (
        min(float(window["start"]) for window in windows),
        max(float(window["end"]) for window in windows),
    )


def plot_reshard_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Stacked bar charts comparing reshard operation duration across runs."""
    valid = results_df[results_df["operation_duration_s"].notna()]
    if valid.empty:
        return

    phase_specs = [
        (
            "up",
            "Reshard-Up Operation Duration",
            [
                ("managed_blackbox_duration_s", "Memorystore black box", "#4c4c4c"),
                ("scale_duration_s", "Scale up", "#64b5cd"),
                ("wait_check_duration_s", "Wait/check", "#8c8c8c"),
                ("slot_move_duration_s", "Move slots", "#dd8452"),
            ],
        ),
        (
            "down",
            "Reshard-Down Operation Duration",
            [
                ("managed_blackbox_duration_s", "Memorystore black box", "#4c4c4c"),
                ("reshard_down_duration_s", "Move slots off", "#8172b2"),
                ("wait_check_duration_s", "Wait/check", "#8c8c8c"),
                ("del_node_duration_s", "Remove nodes", "#ccb974"),
                ("scale_down_duration_s", "Scale down", "#64b5cd"),
            ],
        ),
    ]

    available_specs = [
        (phase, title, segments)
        for phase, title, segments in phase_specs
        if not valid[valid["phase"] == phase].empty
    ]
    if not available_specs:
        return

    fig, axes = plt.subplots(
        1,
        len(available_specs),
        figsize=(max(10, len(available_specs) * 10), 5.2),
        squeeze=False,
        constrained_layout=True,
    )

    for ax, (phase, title, segments) in zip(axes[0], available_specs):
        phase_df = valid[valid["phase"] == phase].copy()
        phase_df["_mode_order"] = phase_df["slot_migration_mode"].map({"legacy": 0, "atomic": 1, "ms": 2}).fillna(-1)
        phase_df = phase_df.sort_values(["run", "_mode_order"])
        _plot_phase_duration_breakdown(ax, phase_df, title, segments)

    fig.savefig(out_dir / "reshard_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    for phase, title, segments in available_specs:
        phase_df = valid[valid["phase"] == phase].copy()
        phase_df["_mode_order"] = phase_df["slot_migration_mode"].map({"legacy": 0, "atomic": 1, "ms": 2}).fillna(-1)
        phase_df = phase_df.sort_values(["run", "_mode_order"])

        fig, ax = plt.subplots(figsize=(13, 5.8), constrained_layout=True)
        _plot_phase_duration_breakdown(ax, phase_df, title, segments)
        fig.savefig(out_dir / f"reshard_{phase}_comparison.png", dpi=180, bbox_inches="tight")
        plt.close(fig)


def _plot_phase_duration_breakdown(
    ax: matplotlib.axes.Axes,
    phase_df: pd.DataFrame,
    title: str,
    segments: List[Tuple[str, str, str]],
) -> None:
    x = _bar_positions(phase_df)
    bottom = np.zeros(len(phase_df))
    totals = phase_df["operation_duration_s"].astype(float).to_numpy()

    for field, label, color in segments:
        values = phase_df[field].fillna(0).astype(float).to_numpy()
        if np.all(values == 0):
            continue

        bars = ax.bar(
            x,
            values,
            width=1.22,
            bottom=bottom,
            color=color,
            edgecolor="black",
            linewidth=0.5,
            label=label,
        )
        for bar, value, start in zip(bars, values, bottom):
            if value >= 3:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    start + value / 2,
                    f"{value:.0f}s",
                    ha="center",
                    va="center",
                    fontsize=8,
                )
        bottom += values

    label_heights = np.maximum(totals, bottom)
    label_offset = max(label_heights.max() * 0.02, 1)
    for idx, total in enumerate(totals):
        ax.text(
            x[idx],
            label_heights[idx] + label_offset,
            f"{total:.0f}s",
            ha="center",
            va="bottom",
            fontsize=8,
            fontweight="bold",
        )

    ax.set_xticks(x)
    ax.set_xticklabels([_run_label(row) for _, row in phase_df.iterrows()])
    ax.set_xlabel("Run")
    ax.set_ylabel("Operation duration (s)")
    ax.set_title(title)
    ax.set_ylim(0, max(label_heights.max() * 1.16, label_heights.max() + 5))
    ax.legend(
        fontsize=8,
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        borderaxespad=0,
        frameon=False,
    )


def _bar_positions(phase_df: pd.DataFrame) -> np.ndarray:
    positions = []
    last_run = object()
    current = -1.65

    for _, row in phase_df.iterrows():
        run = row.get("run")
        current += 1.65
        if positions and run != last_run:
            current += 1.25
        positions.append(current)
        last_run = run

    return np.array(positions)


def _run_label(row: pd.Series) -> str:
    run = str(row.get("run", ""))
    mode = row.get("slot_migration_mode")
    if mode is None or pd.isna(mode):
        return run
    mode_prefix = {
        "legacy": "l",
        "atomic": "a",
        "ms": "m",
    }.get(str(mode), str(mode))
    return f"{mode_prefix}-{run}"


def _plot_stem(value: Any) -> str:
    return str(value).replace("/", "__").replace(" ", "_").replace(":", "_").removesuffix(".json")
