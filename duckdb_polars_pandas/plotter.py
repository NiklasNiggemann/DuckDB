import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_results(output_file, save_fig=False, fig_name="benchmark_lines_stats.png"):
    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"File not found: {output_file}")
    df = pd.read_csv(output_file)
    required_cols = {'backend', 'function', 'run', 'time_s', 'memory_mb'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    sns.set(style="whitegrid", palette="muted", font_scale=1.2)
    df['label'] = df['backend'].astype(str) + ' | ' + df['function'].astype(str)
    df = df.sort_values(by=['label', 'run'])

    unique_labels = df['label'].unique()
    palette = sns.color_palette("husl", len(unique_labels))
    color_dict = dict(zip(unique_labels, palette))

    fig, axes = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
    metrics = [
        ('time_s', 'Time (s)', 'Execution Time per Run'),
        ('memory_mb', 'Memory (MB)', 'Memory Usage per Run')
    ]

    for idx, (metric, ylabel, title) in enumerate(metrics):
        ax = axes[idx]
        for label in unique_labels:
            group = df[df['label'] == label]
            runs = group['run']
            values = group[metric]
            color = color_dict[label]

            # Plot line and points
            ax.plot(runs, values, marker='o', label=label, color=color, linewidth=2, markersize=7)

            # Improved annotation for each point
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

            # Mean, std
            mean = values.mean()
            std = values.std()
            minv, maxv = values.min(), values.max()

            # Error band (mean ± std)
            ax.fill_between(runs, mean - std, mean + std, color=color, alpha=0.15)

            # Mean line
            ax.axhline(mean, linestyle='--', color=color, alpha=0.7)

            # Annotate min and max (no info box)
            ax.scatter(runs.loc[values.idxmin()], minv, color=color, marker='v', s=80, label=None)
            ax.scatter(runs.loc[values.idxmax()], maxv, color=color, marker='^', s=80, label=None)

            # Annotate mean at left edge, aligned with mean line
            xlim = ax.get_xlim()
            ax.text(
                xlim[0] + 0.1 * (xlim[1] - xlim[0]), mean,  # 10% from left
                f"mean={mean:.2f}",
                fontsize=11, color=color, ha='left', va='center', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor=color, alpha=0.8, pad=0.4)
            )

        ax.set_title(title, fontsize=15)
        ax.set_ylabel(ylabel, fontsize=13)
        ax.legend(title='Backend | Function', bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.5)

    axes[1].set_xlabel('Run', fontsize=13)
    plt.tight_layout()

    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")

    plt.show()

def plot_results_multi(csv_files, save_fig=False, fig_name="benchmark_lines_stats.png"):
    dfs = []
    for file in csv_files:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File not found: {file}")
        df = pd.read_csv(file)
        df['source'] = os.path.splitext(os.path.basename(file))[0]
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    required_cols = {'backend', 'function', 'run', 'time_s', 'memory_mb', 'source'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    sns.set(style="whitegrid", palette="muted", font_scale=1.2)
    df['label'] = (
        df['backend'].astype(str) + ' | ' +
        df['function'].astype(str) + ' | ' +
        df['source'].astype(str)
    )
    df = df.sort_values(by=['label', 'run'])

    unique_labels = df['label'].unique()
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
            group = df[df['label'] == label]
            runs = group['run']
            values = group[metric]
            color = color_dict[label]

            # Plot line and points
            ax.plot(runs, values, marker='o', label=label, color=color, linewidth=2, markersize=7)

            # Improved annotation for each point
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

            # Mean, std
            mean = values.mean()
            std = values.std()
            minv, maxv = values.min(), values.max()

            # Error band (mean ± std)
            ax.fill_between(runs, mean - std, mean + std, color=color, alpha=0.15)

            # Mean line
            ax.axhline(mean, linestyle='--', color=color, alpha=0.7)

            # Annotate min and max (no info box)
            ax.scatter(runs.loc[values.idxmin()], minv, color=color, marker='v', s=80, label=None)
            ax.scatter(runs.loc[values.idxmax()], maxv, color=color, marker='^', s=80, label=None)

            # Annotate mean at left edge, aligned with mean line
            xlim = ax.get_xlim()
            ax.text(
                xlim[0] + 0.1 * (xlim[1] - xlim[0]), mean,  # 10% from left
                f"mean={mean:.2f}",
                fontsize=11, color=color, ha='left', va='center', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor=color, alpha=0.8, pad=0.4)
            )

        ax.set_title(title, fontsize=15)
        ax.set_ylabel(ylabel, fontsize=13)
        ax.legend(title='Backend | Function | Source', bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.5)

    axes[1].set_xlabel('Run', fontsize=13)
    plt.tight_layout()

    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")

    plt.show()
