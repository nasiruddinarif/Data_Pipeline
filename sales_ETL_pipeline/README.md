# 🧩 Sales ETL Pipeline (Beginner Data Engineering Project)

## 📘 Overview
This project demonstrates a simple **Extract, Transform, Load (ETL)** pipeline written in Python.  
It reads raw sales data from a CSV file, performs data cleaning and validation, then loads the cleaned data into both CSV and SQLite formats.

---

## ⚙️ Technologies Used
- Python 3.x  
- Pandas  
- Logging module

---

## 🧱 ETL Pipeline Steps

| Step | Function | Description |
|------|-----------|-------------|
| **Extract** | `extract()` | Reads the raw CSV file into a DataFrame. |
| **Transform** | `transform()` | Cleans missing data, renames columns, and calculates total sales. |
| **Validate** | `validate()` | Checks for negative or missing values. |
| **Load** | `load()` | Exports clean data to CSV or SQLite database. |
