import os
from typing import Iterable, List, Optional, Tuple, Dict

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


NUMERIC_COLUMNS = ["scale_factor", "memory_mb", "dataset_size_mb", "time_s", "row_count"]
DEFAULT_MARKER_SIZE = 80


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure key columns are numeric and drop rows with missing required values.

    Required columns to keep a row: memory_mb, dataset_size_mb, time_s, tool, row_count.
    Numeric columns coerced: scale_factor, memory_mb, dataset_size_mb, time_s, row_count.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    df = df.copy()

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    required = ["memory_mb", "dataset_size_mb", "time_s", "tool", "row_count"]
    missing_required = [c for c in required if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    df = df.dropna(subset=required)
    return df

def _build_color_map(df: pd.DataFrame, palette_name: str = "colorblind") -> Dict[str, Tuple[float, float, float]]:
    """
    Create a color map for the tools present in the DataFrame.
    """
    tools = list(pd.Index(df["tool"].unique()).sort_values())
    palette = sns.color_palette(palette_name, n_colors=len(tools))
    return dict(zip(tools, palette))

def load_and_concat_csvs(csv_files: Iterable[str]) -> pd.DataFrame:
    """
    Load and concatenate multiple CSV files into a single DataFrame.

    Raises:
      FileNotFoundError if any file is missing.
      ValueError if no files are provided.
    """
    files = list(csv_files)
    if not files:
        raise ValueError("No CSV files provided.")

    dfs: List[pd.DataFrame] = []
    for file in files:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        dfs.append(pd.read_csv(file))

    if not dfs:
        raise ValueError("No valid CSV files could be read.")

    df = pd.concat(dfs, ignore_index=True)
    return df

def _validate_y_axis(y_axis: str) -> str:
    valid = {"memory_mb", "time_s"}
    if y_axis not in valid:
        raise ValueError(f"y_axis must be one of {valid}, got {y_axis!r}")
    return y_axis

def _format_rows_ticks(ax: plt.Axes, ticks: List[float]) -> None:
    """
    Format the x-axis ticks (row counts) with commas.
    """
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{int(x):,}" for x in ticks], rotation=0, ha="center", fontsize=11)

def _add_secondary_top_axis_dataset_size(ax: plt.Axes, df: pd.DataFrame, ticks: List[float]) -> None:
    """
    Add a secondary x-axis (top) showing dataset size in GB for each tick,
    computed as median dataset_size_mb grouped by row_count.

    If a tick doesn't exist in the data, it will show a placeholder.
    """
    # Prepare mapping row_count -> median dataset_size_mb
    size_by_rows = (
        df.groupby("row_count", as_index=True)["dataset_size_mb"]
        .median()
        .sort_index()
    )

    top_ax = ax.secondary_xaxis("top")
    top_ax.set_xlim(ax.get_xlim())  # align with main axis

    labels = []
    for t in ticks:
        # Best-effort: match exact tick if present; otherwise leave blank
        if t in size_by_rows.index:
            gb = size_by_rows.loc[t] / 1024.0
            labels.append(f"{gb:.2f} GB")
        else:
            labels.append("")  # or use nearest via index.get_indexer if desired

    top_ax.set_xticks(ticks)
    top_ax.set_xticklabels(labels, fontsize=10)
    top_ax.set_xlabel("Dataset Size (GB)")

def plot_scatter_with_trend(
    df: pd.DataFrame,
    y_axis: str = "memory_mb",
    title: Optional[str] = None,
    log_x: bool = True,
    save_path: Optional[str] = None,
    annotate_points: bool = True,
    marker_size: int = DEFAULT_MARKER_SIZE,
    palette_name: str = "colorblind",
    lowess_frac: float = 0.6,
    max_y_ticks: int = 12,  # limit to avoid clutter
) -> Tuple[plt.Figure, plt.Axes]:
    import matplotlib.ticker as mticker

    y_axis = _validate_y_axis(y_axis)
    df = _ensure_numeric(df)

    if df.empty:
        raise ValueError("Dataframe is empty after cleaning.")

    color_map = _build_color_map(df, palette_name=palette_name)

    fig, ax = plt.subplots(figsize=(12, 8))

    for tool in sorted(df["tool"].unique()):
        sub = df[df["tool"] == tool].sort_values("row_count")
        ax.scatter(
            sub["row_count"],
            sub[y_axis],
            s=marker_size,
            c=[color_map.get(tool, (0.5, 0.5, 0.5))],
            label=tool,
            alpha=0.7,
            edgecolor="black",
            linewidths=0.5,
        )

        if len(sub) > 2:
            try:
                from statsmodels.nonparametric.smoothers_lowess import lowess
                lowess_smoothed = lowess(endog=sub[y_axis], exog=sub["row_count"], frac=lowess_frac, return_sorted=True)
                ax.plot(
                    lowess_smoothed[:, 0],
                    lowess_smoothed[:, 1],
                    color=color_map.get(tool, (0.5, 0.5, 0.5)),
                    lw=2,
                    linestyle="--",
                )
            except Exception as e:
                print(f"LOWESS smoothing failed for tool={tool}: {e}")

    # Optional annotations
    if annotate_points:
        for _, row in df.iterrows():
            ax.annotate(
                f"{row[y_axis]}",  # no rounding
                (row["row_count"], row[y_axis]),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7, ec="none"),
            )

    # X-axis label and scale
    ax.set_xlabel("Rows")
    if log_x:
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}" if x > 0 else "0"))
    else:
        ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,}"))

    ax.set_ylabel("Memory Usage (MB)" if y_axis == "memory_mb" else "Execution Time (s)")
    ax.set_title(title or ("Memory Usage" if y_axis == "memory_mb" else "Execution Time"))
    ax.legend(title="Tool", loc="best")

    # Choose representative X ticks from available row_count values
    unique_rows = df[["row_count", "dataset_size_mb"]].drop_duplicates(subset=["row_count"]).sort_values("row_count")
    row_counts = unique_rows["row_count"].to_numpy()
    if len(row_counts) == 0:
        raise ValueError("No valid row_count values to place ticks.")

    step = max(1, len(row_counts) // 8)
    tick_idx = list(range(0, len(row_counts), step))
    row_ticks = row_counts[tick_idx].tolist()
    _format_rows_ticks(ax, row_ticks)

    # Y ticks: only at actual data values (sampled to a reasonable max)
    y_values = sorted(df[y_axis].unique())
    if len(y_values) > max_y_ticks:
        # sample evenly across the sorted unique values
        idx = [int(i) for i in np.linspace(0, len(y_values) - 1, max_y_ticks)]
        y_ticks = [y_values[i] for i in idx]
    else:
        y_ticks = y_values

    ax.set_yticks(y_ticks)
    # Y-axis: let matplotlib choose logical intervals
    if y_axis == "memory_mb":
        ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=max_y_ticks, integer=False, prune=None))
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
    else:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=max_y_ticks, integer=False, prune=None))
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))

    # Grid only at the tick positions (i.e., only where there are values/ticks)
    ax.grid(True, axis="both", which="major", linestyle="--", alpha=0.3)

    # Secondary top axis for dataset size (GB)
    _add_secondary_top_axis_dataset_size(ax, df, row_ticks)

    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Plot saved to {save_path}")

    return fig, ax

def plot_benchmark(csv_files: Iterable[str], y_axis: str = "memory_mb", **plot_kwargs) -> Tuple[plt.Figure, plt.Axes]:
    df = load_and_concat_csvs(csv_files)
    return plot_scatter_with_trend(df, y_axis=y_axis, **plot_kwargs)

