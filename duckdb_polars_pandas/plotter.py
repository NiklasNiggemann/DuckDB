import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def _load_and_concat_csvs(csv_files, add_source=False):
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

def _check_required_columns(df, required_cols):
    """Ensure required columns are present in DataFrame."""
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")

def barcharts(csv_files, save_fig=False, fig_name="benchmark_barcharts.png"):
    df = _load_and_concat_csvs(csv_files, add_source=True)
    _check_required_columns(df, {'tool', 'function', 'run', 'time_s', 'memory_mb', 'source'})

    tools = ['duckdb', 'polars', 'pandas']
    palette = sns.color_palette("husl", len(tools))
    color_dict = dict(zip(tools, palette))
    bar_colors = [color_dict[tool] for tool in tools]

    avg_times = df.groupby('tool')['time_s'].mean().reindex(tools).fillna(0)
    avg_memory = df.groupby('tool')['memory_mb'].mean().reindex(tools).fillna(0)

    fig, axes = plt.subplots(1, 2, figsize=(12, 6))

    axes[0].bar(tools, avg_times, color=bar_colors)
    axes[0].set_ylabel('Average Time (s)')
    axes[0].set_title('Average Execution Time by Tool')
    axes[0].set_ylim(bottom=0)
    for i, v in enumerate(avg_times):
        axes[0].text(i, v + 0.01, f"{v:.2f}", ha='center', va='bottom')

    axes[1].bar(tools, avg_memory, color=bar_colors)
    axes[1].set_ylabel('Average Memory (MB)')
    axes[1].set_title('Average Memory Usage by Tool')
    axes[1].set_ylim(bottom=0)
    for i, v in enumerate(avg_memory):
        axes[1].text(i, v + 0.01, f"{v:.2f}", ha='center', va='bottom')

    plt.tight_layout()

    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")

    plt.show()

def _plot_lines(df, label_col, legend_title, save_fig=False, fig_name="benchmark_lines_stats.png"):
    """
    Generalized line plot for time and memory metrics.
    """
    sns.set(style="whitegrid", palette="muted", font_scale=1.2)
    df = df.sort_values(by=[label_col, 'run'])
    unique_labels = df[label_col].unique()
    palette = sns.color_palette("husl", len(unique_labels))
    color_dict = dict(zip(unique_labels, palette))

    fig, axes = plt.subplots(2, 1, figsize=(16, 13), sharex=True)
    metrics = [
        ('time_s', 'Time (s)', 'Execution Time per Run'),
        ('memory_mb', 'Memory (MB)', 'Memory Usage per Run')
    ]

    for idx, (metric, ylabel, title) in enumerate(metrics):
        ax = axes[idx]
        for label in unique_labels:
            group = df[df[label_col] == label]
            runs = group['run']
            values = group[metric]
            color = color_dict[label]

            ax.plot(runs, values, marker='o', label=label, color=color, linewidth=2, markersize=7)

            y_range = values.max() - values.min()
            offset = y_range * 0.04 if y_range > 0 else 0.5
            for i, (x, y) in enumerate(zip(runs, values)):
                va = 'bottom' if i % 2 == 0 else 'top'
                y_text = y + offset if va == 'bottom' else y - offset
                ax.text(
                    x, y_text, f"{y:.2f}",
                    fontsize=9, color=color, ha='center', va=va, fontweight='bold',
                    bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=0.5)
                )

            mean = values.mean()
            std = values.std()
            minv, maxv = values.min(), values.max()

            ax.fill_between(runs, mean - std, mean + std, color=color, alpha=0.15)
            ax.axhline(mean, linestyle='--', color=color, alpha=0.7)
            ax.scatter(runs.loc[values.idxmin()], minv, color=color, marker='v', s=80, label=None)
            ax.scatter(runs.loc[values.idxmax()], maxv, color=color, marker='^', s=80, label=None)

            xlim = ax.get_xlim()
            ax.text(
                xlim[0] + 0.1 * (xlim[1] - xlim[0]), mean,
                f"mean={mean:.2f}",
                fontsize=11, color=color, ha='left', va='center', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor=color, alpha=0.8, pad=0.4)
            )

        ax.set_title(title, fontsize=15)
        ax.set_ylabel(ylabel, fontsize=13)
        ax.legend(title=legend_title, bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.5)

    axes[1].set_xlabel('Run', fontsize=13)
    plt.tight_layout()

    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")

    plt.show()

def plot_results(output_file, save_fig=False, fig_name="benchmark_lines_stats.png"):
    """
    Plot time and memory usage per run for a single CSV file.
    """
    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"File not found: {output_file}")
    df = pd.read_csv(output_file)
    _check_required_columns(df, {'tool', 'function', 'run', 'time_s', 'memory_mb'})
    df['label'] = df['tool'].astype(str) + ' | ' + df['function'].astype(str)
    _plot_lines(df, 'label', 'tool | Function', save_fig, fig_name)

def plot_results_multi(csv_files, save_fig=False, fig_name="benchmark_lines_stats.png"):
    """
    Plot time and memory usage per run for multiple CSV files.
    """
    df = _load_and_concat_csvs(csv_files, add_source=True)
    _check_required_columns(df, {'tool', 'function', 'run', 'time_s', 'memory_mb', 'source'})
    df['label'] = (
        df['tool'].astype(str) + ' | ' +
        df['function'].astype(str) + ' | '
    )
    _plot_lines(df, 'label', 'Tool | Function', save_fig, fig_name)
