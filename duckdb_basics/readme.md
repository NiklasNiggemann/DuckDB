# DuckDB OLAP Demo on E-Commerce Behavior Data
This project demonstrates basic OLAP (Online Analytical Processing) operations on the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) using DuckDB in Python.

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

See requirements.txt and install with:  
```bash 
pip install -r requirements.txt
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

## Extending

To add your own OLAP query:
1. Define a new function in `main.py`.
2. Use DuckDB SQL to query the CSV.
3. Pass your function to `main()` at the bottom of the script.
