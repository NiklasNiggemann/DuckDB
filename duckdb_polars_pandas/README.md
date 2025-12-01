# DuckDB vs. DataFrame Libraries

A Python benchmarking tool for comparing memory usage and execution time across popular data processing libraries: **Pandas**, **Polars**, and **DuckDB**. Results are logged, exported to CSV, and visualized with Matplotlib.

## Features

- **Benchmark multiple tools**: Pandas, Polars, DuckDB
- **Functions tested**: Filtering, grouping, aggregation
- **Modes**: Cold (fresh process per run) and Hot (multiple runs in one process)
- **Automatic CSV export** of results
- **Summary statistics** (mean, std, CV, min, max, span)
- **Publication-ready plots**: Line plots, bar charts, hot vs cold comparisons
- **All output logged** to `results/benchmark_log.txt`

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/benchmark-runner.git
    cd benchmark-runner
    ```

2. **Install dependencies**:
    ```bash
    pip install requirements.txt
    ```

## Usage

Run the benchmark runner from the command line:

```bash
python benchmark_runner.py --tool all --function all --mode all --runs 10
```

### Arguments

| Argument      | Choices                                                                 | Description                                 |
|---------------|------------------------------------------------------------------------|---------------------------------------------|
| `--tool`      | `all`, `duckdb_polars`, `duckdb`, `polars`, `pandas`                   | Which tools to benchmark                    |
| `--function`  | `all`, `filtering_counting`, `filtering_grouping_aggregation`, `grouping_conditional_aggregation` | Which function(s) to test                   |
| `--mode`      | `all`, `cold`, `hot`                                                   | Benchmark mode                              |
| `--runs`      | Integer                                                                | Number of runs per benchmark                |

### Example: Benchmark Pandas only

```bash
python benchmark_runner.py --tool pandas --function filtering_counting --mode cold --runs 5
```

## Output

- **CSV files**: Saved in `results/`, e.g. `pandas_filtering_counting_cold.csv`
- **Plots**: Saved as PNGs in `results/`
- **Log file**: All output and errors in `results/benchmark_log.txt`

## Plotting

Plots are generated automatically after benchmarks:
- **Line plots**: Per tool/function/mode
- **Bar charts**: Compare tools for a function/mode
- **Hot vs Cold**: Compare modes for each tool/function

## Logging

All output (including errors) is logged to `results/benchmark_log.txt` for reproducibility.

## Contributing

Pull requests and issues are welcome!
