Here’s a **tailored README.md** for your specific code and project structure:

---

```markdown
# DuckDB OLAP Demo on E-Commerce Behavior Data

This project demonstrates basic OLAP (Online Analytical Processing) operations on the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) using [DuckDB](https://duckdb.org/) in Python.

## Project Overview

- **Performs OLAP-style queries** (slice, dice, roll-up, drill-down, pivot) on e-commerce event data.
- **Measures query execution time** using a custom `Timer` context manager.
- **Easy to extend** with your own queries.

## Dataset

Download `eCommerce.csv` from the Kaggle dataset and place it in the `datasets/` directory, which should be one level above your project folder.

```
your_project/
├── duckdb_basics/
│   └── main.py
└── datasets/
    └── eCommerce.csv
```

## Requirements

- Python 3.7+
- [DuckDB Python package](https://pypi.org/project/duckdb/)

Install DuckDB:
```bash
pip install duckdb
```

## Usage

Edit the last line of `main.py` to select which function to run, for example:

```python
main(purchases_and_count)
```

Then run:

```bash
python main.py
```

Each query will print its results and the time taken.

## Available OLAP Functions

- **purchases_and_count:**  
  Show all purchase events and their total count.

- **purchases_samsung_and_count:**  
  Show all Samsung smartphone purchases and their total count.

- **total_sales_per_category:**  
  Show total sales (sum of price) per product category.

- **total_sales_per_category_by_brand:**  
  Show total sales per category and brand.

- **purchases_per_event_by_category:**  
  Show a pivot table: views, carts, and purchases per category.

## Example Output

```
category_code           views   carts   purchases
electronics.smartphone  10000   500     200
...
2.13 seconds elapsed
```

## Extending

To add your own OLAP query:
1. Define a new function in `main.py`.
2. Use DuckDB SQL to query the CSV.
3. Pass your function to `main()` at the bottom of the script.

## License

MIT License

## Credits

- [DuckDB](https://duckdb.org/)
- [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
```

---

**Copy and save this as `README.md` in your project directory.**  
Let me know if you want to add more usage examples or further details!