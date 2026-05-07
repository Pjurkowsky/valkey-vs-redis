"""Shared chart annotations for time-series test plots."""

from typing import Optional

import matplotlib.axes


def mark_test_window(
    ax: matplotlib.axes.Axes,
    start_s: Optional[float],
    end_s: Optional[float],
    *,
    with_labels: bool = False,
    start_label: str = "Test start",
    end_label: str = "Test end",
) -> bool:
    """Mark the start and end of the actual injected test event."""
    if start_s is None:
        return False

    start = float(start_s)
    end = float(end_s) if end_s is not None else start

    ax.axvline(
        start,
        linestyle="-.",
        color="#2ca02c",
        linewidth=1.0,
        label=start_label if with_labels else None,
    )

    if end != start:
        ax.axvline(
            end,
            linestyle="-.",
            color="#d62728",
            linewidth=1.0,
            label=end_label if with_labels else None,
        )

    return True
