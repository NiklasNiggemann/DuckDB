import os
import re
from typing import List, Set
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns

TOOLS = ['duckdb', 'polars', 'pandas']
PALETTE = sns.color_palette("colorblind", len(TOOLS))
COLOR_DICT = dict(zip(TOOLS, PALETTE))

def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce run/time/memory to numeric and drop rows with missing values."""
    df = df.copy()
    df['run'] = pd.to_numeric(df['run'], errors='coerce')
    df['time_s'] = pd.to_numeric(df['time_s'], errors='coerce')
    df['memory_mb'] = pd.to_numeric(df['memory_mb'], errors='coerce')
    df = df.dropna(subset=['run', 'time_s', 'memory_mb'])
    return df

def _moving_average(series: pd.Series, window: int) -> pd.Series:
    """Compute moving average with a centered window when possible."""
    if window <= 1:
        return series
    return series.rolling(window=window, min_periods=1, center=True).mean()

def plot_lines(
    df: pd.DataFrame,
    label_col: str,
    legend_title: str,
    save_fig: bool = False,
    fig_name: str = "benchmark_lines_stats.png",
    annotate_points: bool = True,
    show_std_band: bool = True,
    show_mean_line: bool = True,
    smoothing_window: int = 1,
    figsize: tuple = (16, 10),
    linewidth: float = 2.0,
    markersize: float = 6.0,
    grid_alpha: float = 0.3,
    y_pad_ratio: float = 0.06,
    time_ylabel: str = "Execution Time (s)",
    mem_ylabel: str = "Memory Usage (MB)",
    title_time: str = "Execution Time per Run",
    title_mem: str = "Memory Usage per Run",
    use_log_y_for_time: bool = False,
    use_log_y_for_memory: bool = False,
    use_symlog_for_time: bool = False,
    use_symlog_for_memory: bool = False,
    symlog_linthresh: float = 1.0,
) -> None:
    """
    Generalized line plot for time and memory metrics with robust side boxes:
    - Left column: line charts
    - Right column: each row split into Legend (top) + Mean table (bottom)
    """
    df = _ensure_numeric(df)
    if df.empty:
        print("plot_lines: No numeric data to plot.")
        return

    sns.set_theme(style="whitegrid", context="talk")
    df = df.sort_values(by=[label_col, 'run'])
    unique_labels = list(df[label_col].unique())

    def label_color(group: pd.DataFrame) -> str:
        tool_name = str(group['tool'].iloc[0]) if not group.empty else ""
        return COLOR_DICT.get(tool_name, "#333333")

    # 2x2 grid: left (plots), right (legend+table per row), right column slightly narrower
    fig = plt.figure(figsize=figsize)
    gs = gridspec.GridSpec(2, 2, width_ratios=[1.0, 0.42], wspace=0.18, hspace=0.32)

    # Main plot axes
    ax_time = fig.add_subplot(gs[0, 0])
    ax_mem = fig.add_subplot(gs[1, 0])

    # For each right cell, create a sub-grid: [legend, table]
    right_time = gridspec.GridSpecFromSubplotSpec(
        2, 1, subplot_spec=gs[0, 1], height_ratios=[0.45, 0.55], hspace=0.05
    )
    side_time_leg = fig.add_subplot(right_time[0])
    side_time_tbl = fig.add_subplot(right_time[1])

    right_mem = gridspec.GridSpecFromSubplotSpec(
        2, 1, subplot_spec=gs[1, 1], height_ratios=[0.45, 0.55], hspace=0.05
    )
    side_mem_leg = fig.add_subplot(right_mem[0])
    side_mem_tbl = fig.add_subplot(right_mem[1])

    # Turn off axes visuals for the side panels
    for ax in [side_time_leg, side_time_tbl, side_mem_leg, side_mem_tbl]:
        ax.set_axis_off()

    metrics = [
        ('time_s', time_ylabel, title_time, use_log_y_for_time, use_symlog_for_time, ax_time, (side_time_leg, side_time_tbl)),
        ('memory_mb', mem_ylabel, title_mem, use_log_y_for_memory, use_symlog_for_memory, ax_mem, (side_mem_leg, side_mem_tbl)),
    ]

    for metric, ylabel, title, use_log_y, use_symlog, ax, (ax_leg, ax_tbl) in metrics:
        # Plot each label line
        for label in unique_labels:
            group = df[df[label_col] == label]
            if group.empty:
                continue
            runs = group['run'].astype(int)
            values = group[metric].astype(float)
            color = label_color(group)

            ax.plot(
                runs, values,
                marker='o', label=label, color=color,
                linewidth=linewidth, markersize=markersize, alpha=0.95
            )

            # Smoothing overlay
            if smoothing_window and smoothing_window > 1:
                smoothed = _moving_average(values, smoothing_window)
                ax.plot(
                    runs, smoothed,
                    color=color, linewidth=linewidth + 0.4,
                    alpha=0.7, linestyle='-', label=None
                )

            # Point annotations
            if annotate_points:
                y_range = values.max() - values.min()
                offset = max(abs(values.mean()) * 0.02, y_range * 0.04, 0.5)
                for i, (x, y) in enumerate(zip(runs, values)):
                    va = 'bottom' if i % 2 == 0 else 'top'
                    y_text = y + offset if va == 'bottom' else y - offset
                    ax.text(
                        x, y_text, f"{y:.2f}",
                        fontsize=9, color=color, ha='center', va=va, fontweight='bold',
                        bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=0.3)
                    )

            # Stats overlays
            mean = values.mean()
            std = values.std()
            if show_std_band and std > 0:
                ax.fill_between(runs, mean - std, mean + std, color=color, alpha=0.14, label=None)
            if show_mean_line:
                ax.axhline(mean, linestyle='--', color=color, alpha=0.7, linewidth=1.2, label=None)

            # Mark min/max
            min_idx = values.idxmin()
            max_idx = values.idxmax()
            ax.scatter(group['run'].loc[min_idx], values.loc[min_idx], color=color, marker='v', s=80, label=None)
            ax.scatter(group['run'].loc[max_idx], values.loc[max_idx], color=color, marker='^', s=80, label=None)

        # Axis formatting
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(True, linestyle='--', alpha=grid_alpha)

        # Scale selection
        if use_symlog:
            ax.set_yscale('symlog', linthresh=symlog_linthresh)
        elif use_log_y:
            ax.set_yscale('log')

        # Compute y-limits with padding, allowing negatives when scale permits
        y_min = df[metric].min()
        y_max = df[metric].max()
        pad = (y_max - y_min) * y_pad_ratio if y_max != y_min else max(abs(y_max) * y_pad_ratio, 1e-6)

        if use_log_y:
            # Log scale cannot show zero or negatives
            lower = max(1e-12, y_min - pad)
        else:
            lower = y_min - pad

        ax.set_ylim(lower, y_max + pad)

        ax.get_yaxis().set_major_formatter(
            plt.FuncFormatter(lambda x, p: f"{x:,.2f}" if (y_max - y_min) < 1000 else f"{x:,.0f}")
        )

        # Build legend handles: one per label, colored by tool
        legend_handles = []
        for label in unique_labels:
            sub = df[df[label_col] == label]
            if sub.empty:
                continue
            tool_name = sub['tool'].iloc[0]
            color = COLOR_DICT.get(tool_name, "#333333")
            legend_handles.append(mpatches.Patch(color=color, label=label))

        # Draw legend in its dedicated axis
        leg = ax_leg.legend(
            handles=legend_handles,
            title=legend_title,
            loc='upper left',
            frameon=False,
            ncol=1
        )

        # Mean panel: Tool | Mean (grouped by tool)
        means_by_tool = df.groupby('tool')[metric].mean().reindex(TOOLS).dropna()
        rows = [(tool, f"{means_by_tool.loc[tool]:.2f}") for tool in means_by_tool.index]
        colors = [COLOR_DICT.get(tool, "#333333") for tool in means_by_tool.index]

        # Draw compact table in ax_tbl (axes coords)
        # Header
        y = 0.98
        line_h = 0.18  # spacing between rows in axes coords
        ax_tbl.text(0.0, y, "Mean by Tool", ha='left', va='top', fontsize=11, fontweight='bold', transform=ax_tbl.transAxes)
        y -= 0.18

        # Rows
        for (tool_name, mean_text), color in zip(rows, colors):
            # color square
            ax_tbl.add_patch(
                mpatches.Rectangle(
                    (0.0, y - 0.035), 0.035, 0.07,
                    transform=ax_tbl.transAxes, color=color, clip_on=False
                )
            )
            # tool name
            ax_tbl.text(0.06, y, tool_name, ha='left', va='center', fontsize=10, color='#222222', transform=ax_tbl.transAxes)
            # mean value
            ax_tbl.text(0.98, y, mean_text, ha='right', va='center', fontsize=10, color='#222222', transform=ax_tbl.transAxes)
            y -= line_h

    ax_mem.set_xlabel('Run')
    plt.tight_layout()
    if save_fig:
        fig.savefig(fig_name, dpi=180, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")
    plt.show()

def plot_results(
    output_file: str,
    save_fig: bool = False,
    fig_name: str = "benchmark_lines_stats.png",
    **plot_kwargs
) -> None:
    """Plot time and memory usage per run for a single CSV file."""
    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"File not found: {output_file}")
    df = pd.read_csv(output_file)
    check_required_columns(df, {'tool', 'run', 'time_s', 'memory_mb'})
    df['label'] = df['tool'].astype(str)
    plot_lines(df, 'label', 'Tool', save_fig, fig_name, **plot_kwargs)

def plot_results_multi(
    csv_files: List[str],
    save_fig: bool = False,
    fig_name: str = "benchmark_lines_stats.png",
    **plot_kwargs
) -> None:
    """Plot time and memory usage per run for multiple CSV files."""
    df = load_and_concat_csvs(csv_files, add_source=True)
    check_required_columns(df, {'tool', 'run', 'time_s', 'memory_mb', 'source'})
    df['label'] = df['tool'].astype(str)
    plot_lines(df, 'label', 'Tool', save_fig, fig_name, **plot_kwargs)

def load_and_concat_csvs(csv_files: List[str], add_source: bool = False) -> pd.DataFrame:
    """Load multiple CSV files and concatenate into a single DataFrame."""
    dfs = []
    for file in csv_files:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        df = pd.read_csv(file)
        if add_source:
            df['source'] = os.path.splitext(os.path.basename(file))[0]
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def check_required_columns(df: pd.DataFrame, required_cols: Set[str]) -> None:
    """Ensure required columns are present in DataFrame."""
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

def barcharts(
    csv_files: List[str],
    save_fig: bool = False,
    fig_name: str = "benchmark_barcharts.png",
    tools: List[str] = TOOLS
) -> None:
    """Plot average time and memory usage bar charts for each tool."""
    df = load_and_concat_csvs(csv_files, add_source=True)
    check_required_columns(df, {'tool', 'run', 'time_s', 'memory_mb', 'source'})

    bar_colors = [COLOR_DICT.get(tool, "#333333") for tool in tools]
    avg_times = df.groupby('tool')['time_s'].mean().reindex(tools).fillna(0)
    avg_memory = df.groupby('tool')['memory_mb'].mean().reindex(tools).fillna(0)

    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    for ax, metric, ylabel, title, values in zip(
        axes,
        ['time_s', 'memory_mb'],
        ['Average Time (s)', 'Average Memory (MB)'],
        ['Average Execution Time by Tool', 'Average Memory Usage by Tool'],
        [avg_times, avg_memory]
    ):
        ax.bar(tools, values, color=bar_colors)
        ax.set_ylabel(ylabel)
        ax.set_title(title)

        # Allow negatives with padding
        y_min = values.min()
        y_max = values.max()
        pad = (y_max - y_min) * 0.06 if y_max != y_min else 1.0
        ax.set_ylim(y_min - pad, y_max + pad)

        for i, v in enumerate(values):
            ax.text(i, v + (abs(v) * 0.02 if v != 0 else 0.01), f"{v:.2f}", ha='center', va='bottom')

    plt.tight_layout()
    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")
    plt.show()

def barcharts_hot_vs_cold(
    csv_files: List[str],
    save_fig: bool = False,
    fig_name: str = "benchmark_hot_vs_cold_barcharts.png",
    tools: List[str] = TOOLS,
    modes: List[str] = ['cold', 'hot']
) -> None:
    """Plot grouped bar charts comparing hot and cold runs for each tool, using tool colors."""
    df = load_and_concat_csvs(csv_files, add_source=True)
    check_required_columns(df, {'tool', 'run', 'time_s', 'memory_mb', 'source'})

    # Derive mode from filename and normalize missing
    mode_re = re.compile(r'_(cold|hot)\b')
    df['mode'] = df['source'].apply(lambda x: (mode_re.search(x).group(1) if mode_re.search(x) else 'unknown'))

    # Aggregate
    avg_times = df.groupby(['tool', 'mode'])['time_s'].mean().unstack().reindex(tools).fillna(0)
    avg_memory = df.groupby(['tool', 'mode'])['memory_mb'].mean().unstack().reindex(tools).fillna(0)

    # Ensure only requested modes columns are present (in order)
    present_modes = [m for m in modes if m in avg_times.columns]
    if not present_modes:
        raise ValueError("No requested modes found in data for hot vs cold barcharts.")
    avg_times = avg_times.reindex(columns=present_modes).fillna(0)
    avg_memory = avg_memory.reindex(columns=present_modes).fillna(0)

    # Plot setup
    import numpy as np
    sns.set_theme(style="whitegrid", context="talk")
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=False)

    # Common positions
    x = np.arange(len(tools))
    width = 0.42 if len(present_modes) == 2 else 0.6  # bar width
    offsets = {
        1: [0.0],
        2: [-width/2, width/2],
        3: [-width, 0.0, width]
    }[len(present_modes)]

    # Visual differentiation for modes
    mode_style = {
        'cold': {'alpha': 0.65, 'hatch': '//'},
        'hot':  {'alpha': 0.95, 'hatch': ''},
        'unknown': {'alpha': 0.8, 'hatch': 'xx'}
    }

    def draw_grouped(ax, values_df, ylabel, title):
        containers = []
        for i, mode in enumerate(present_modes):
            vals = values_df[mode].values
            pos = x + offsets[i]
            colors = [COLOR_DICT.get(t, "#666666") for t in tools]
            style = mode_style.get(mode, mode_style['unknown'])
            bars = ax.bar(
                pos, vals,
                width=width,
                color=colors,
                alpha=style['alpha'],
                hatch=style['hatch'],
                edgecolor='black',
                linewidth=0.5,
                label=mode.capitalize()
            )
            containers.append(bars)

            # Value labels on top of bars
            top_pad = (max(vals) * 0.02 if max(vals) != 0 else 0.01)
            for bx, v in zip(pos, vals):
                ax.text(
                    bx, v + (abs(v) * 0.02 if v != 0 else top_pad),
                    f"{v:.2f}",
                    ha='center', va='bottom', fontsize=9
                )

        ax.set_xticks(x)
        ax.set_xticklabels(tools)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, axis='y', linestyle='--', alpha=0.3)

        # Allow negatives with padding across all values and modes
        y_min = values_df.min().min()
        y_max = values_df.max().max()
        pad = (y_max - y_min) * 0.06 if y_max != y_min else 1.0
        ax.set_ylim(y_min - pad, y_max + pad)

        # Legend for modes (hatch/alpha distinction)
        ax.legend(title='Mode', loc='upper right')

    draw_grouped(axes[0], avg_times, 'Average Time (s)', 'Average Execution Time by Tool (Hot vs Cold)')
    draw_grouped(axes[1], avg_memory, 'Average Memory (MB)', 'Average Memory Usage by Tool (Hot vs Cold)')

    # Optional: add a small legend mapping tool -> color (outside figure)
    tool_patches = [mpatches.Patch(color=COLOR_DICT.get(t, "#666666"), label=t) for t in tools]
    fig.legend(handles=tool_patches, title='Tool Colors', loc='center right', bbox_to_anchor=(1.02, 0.5))

    plt.tight_layout()
    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")
    plt.show()
