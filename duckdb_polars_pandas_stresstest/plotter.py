import os
from typing import List
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

TOOLS = ['duckdb', 'polars', 'pandas']
PALETTE = sns.color_palette("colorblind", len(TOOLS))
COLOR_DICT = dict(zip(TOOLS, PALETTE))

def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ['scale_factor', 'memory_mb', 'dataset_size_mb', 'time_s']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['memory_mb', 'dataset_size_mb', 'time_s', 'tool'])

def load_and_concat_csvs(csv_files: List[str]) -> pd.DataFrame:
    dfs = []
    for file in csv_files:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        dfs.append(pd.read_csv(file))
    return pd.concat(dfs, ignore_index=True)

def plot_memory_and_time_vs_dataset_size(
    df: pd.DataFrame,
    tools: List[str] = TOOLS,
    save_fig: bool = False,
    fig_name: str = "memory_and_time_vs_dataset_size.png",
    x_axis: str = "dataset_size_mb"  # or "row_count"
):
    if x_axis not in df.columns or 'time_s' not in df.columns:
        print("Required columns not found.")
        return

    df = _ensure_numeric(df)
    sns.set_theme(style="whitegrid", context="talk")
    fig, axes = plt.subplots(2, 1, figsize=(10, 12), sharex=True)

    # --- Memory Usage Plot ---
    ax = axes[0]
    for tool in tools:
        sub = df[df['tool'] == tool].sort_values(x_axis)
        if sub.empty:
            continue
        ax.plot(
            sub[x_axis], sub['memory_mb'],
            marker='o', label=tool, color=COLOR_DICT.get(tool, "#333333"),
            linewidth=2.0, markersize=7, alpha=0.95
        )
        for x, y, rc in zip(sub[x_axis], sub['memory_mb'], sub['row_count']):
            ax.text(
                x, y, f"{y:.1f}\n({rc:,} rows)", fontsize=9, color=COLOR_DICT.get(tool, "#333333"),
                ha='center', va='bottom', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=0.3),
                clip_on=True
            )
    ax.set_ylabel("Memory Usage (MB)")
    ax.set_title(f"Memory Usage vs {x_axis.replace('_', ' ').title()}")
    ax.legend(title="Tool")
    ax.grid(True, linestyle='--', alpha=0.3)

    # --- Execution Time Plot ---
    ax = axes[1]
    for tool in tools:
        sub = df[df['tool'] == tool].sort_values(x_axis)
        if sub.empty:
            continue
        ax.plot(
            sub[x_axis], sub['time_s'],
            marker='o', label=tool, color=COLOR_DICT.get(tool, "#333333"),
            linewidth=2.0, markersize=7, alpha=0.95
        )
        for x, y, rc in zip(sub[x_axis], sub['time_s'], sub['row_count']):
            ax.text(
                x, y, f"{y:.2f}\n({rc:,} rows)", fontsize=9, color=COLOR_DICT.get(tool, "#333333"),
                ha='center', va='bottom', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=0.3),
                clip_on=True
            )
    ax.set_xlabel(x_axis.replace('_', ' ').title())
    ax.set_ylabel("Execution Time (s)")
    ax.set_title(f"Execution Time vs {x_axis.replace('_', ' ').title()}")
    ax.legend(title="Tool")
    ax.grid(True, linestyle='--', alpha=0.3)

    plt.tight_layout()
    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")
    plt.show()

def plot_benchmark(csv_files: List[str], tools: List[str] = TOOLS, save_fig: bool = False, fig_name: str = "memory_and_time_vs_dataset_size.png"):
    df = load_and_concat_csvs(csv_files)
    plot_memory_and_time_vs_dataset_size(df, tools=tools, save_fig=save_fig, fig_name=fig_name)

