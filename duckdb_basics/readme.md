# DuckDB OLAP Demo on E-Commerce Behavior Data

This project demonstrates basic OLAP (Online Analytical Processing) operations on the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) using DuckDB in Python.

---

## 📝 Project Overview

- **OLAP-style queries:**  
  Perform slice, dice, roll-up, drill-down, and pivot operations on e-commerce event data.
- **Query timing:**  
  Measure execution time with a custom `Timer` context manager.
- **Easily extensible:**  
  Add your own queries with minimal effort.

---

## 📂 Dataset

This project uses the **eCommerce Behavior Data** from Kaggle.

**Setup:**
1. Download `eCommerce.csv` from the [Kaggle dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
2. Place the file in the `datasets/` directory, which should be one level above your project folder:

```
your_project/
├── duckdb_basics/
│   └── main.py
└── datasets/
    └── eCommerce.csv
```

---

## ⚙️ Requirements

Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

---

## 🚀 Usage

1. **Select a function:**  
   Edit the last line of `main.py` to choose which OLAP function to run, for example:

   ```python
   main(purchases_and_count)
   ```

2. **Run the script:**

   ```bash
   python main.py
   ```

   Each query prints its results and the time taken.

---

## 📊 Available OLAP Functions

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

---

## ➕ Extending

To add your own OLAP query:

1. Define a new function in `main.py`.
2. Use DuckDB SQL to query the CSV.
3. Pass your function to `main()` at the bottom of the script.

---

**Note:**  
This project exclusively uses the eCommerce dataset from Kaggle for all OLAP demonstrations.

---

Feel free to contribute new queries or suggest improvements!