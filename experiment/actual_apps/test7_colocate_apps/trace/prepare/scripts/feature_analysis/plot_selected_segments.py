import csv
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp")

import matplotlib.pyplot as plt


SELECTED_SEGMENTS = {
    "highload": [(3, 4), (13, 14), (35, 36), (49, 50), (53, 54)],
    "lowload": [(3, 4), (17, 18), (35, 36), (39, 40), (59, 60)],
}

COLORS = {
    "highload": "#D55E00",
    "lowload": "#0072B2",
}


script_dir = Path(__file__).parent.resolve()
prepare_dir = script_dir.parents[1]
rpm_dir = prepare_dir / "rpm"
output_path = rpm_dir / "highload_lowload_selected_segments.png"


def load_rpm(trace_name):
    csv_path = rpm_dir / f"{trace_name}.csv"
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((int(row["Minute"]), int(float(row["RPM"]))))
    return rows


def draw_selected_windows(ax, trace_name, y_offset=0):
    color = COLORS[trace_name]
    for start_minute, end_minute in SELECTED_SEGMENTS[trace_name]:
        ax.axvspan(start_minute - 0.5, end_minute + 0.5, color=color, alpha=0.12)
        ax.text(
            (start_minute + end_minute) / 2,
            ax.get_ylim()[1] - y_offset,
            f"{start_minute}-{end_minute}",
            color=color,
            ha="center",
            va="top",
            fontsize=8,
            fontweight="bold",
        )


def plot():
    fig, ax = plt.subplots(figsize=(13, 6))

    all_values = []
    for trace_name in ("highload", "lowload"):
        rows = load_rpm(trace_name)
        minutes = [minute for minute, _ in rows]
        rpm = [value for _, value in rows]
        all_values.extend(rpm)
        ax.plot(
            minutes,
            rpm,
            color=COLORS[trace_name],
            linewidth=1.8,
            marker="o",
            markersize=3,
            label=f"{trace_name} RPM",
        )

    ax.set_xlim(1, 60)
    ax.set_ylim(0, max(all_values) * 1.12)
    ax.set_xlabel("Minute")
    ax.set_ylabel("RPM")
    ax.set_title("Highload/Lowload RPM and Selected 2-Minute Segments")
    ax.grid(True, alpha=0.25)

    draw_selected_windows(ax, "highload", y_offset=max(all_values) * 0.03)
    draw_selected_windows(ax, "lowload", y_offset=max(all_values) * 0.10)

    ax.legend(loc="upper right")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160)
    print(f"Saved plot to {output_path}")


if __name__ == "__main__":
    plot()
