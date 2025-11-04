# 🧩 E-Commerce Orders ETL Pipeline

This project demonstrates a complete **ETL (Extract, Transform, Validate, Load)** data pipeline built using **Python (Pandas & Logging)**.  
The pipeline processes raw e-commerce order and customer data, performs transformations, validates data quality, and outputs a clean dataset ready for analysis.

---

## 🧱 Project Overview

This ETL pipeline simulates a real-world data engineering workflow used in e-commerce systems.  
It performs the following key tasks:

1. **Extract** data from multiple CSV sources (`orders.csv`, `customers.csv`)
2. **Transform** data by:
   - Merging datasets  
   - Cleaning missing values  
   - Calculating new metrics (e.g., `Total_Value`)  
   - Parsing and enriching dates  
3. **Validate** data for missing, duplicate, and invalid records  
4. **Load** the final clean dataset into a new CSV file  

---

## ⚙️ ETL Pipeline Components

### 1. 🧲 **Extract**
- Reads two CSV files:
  - `orders.csv` — transactional order data
  - `customers.csv` — customer information  
- Logs file size and extraction duration.

### 2. 🧪 **Transform**
- Merges `orders` and `customers` datasets on `Customer_ID`
- Handles missing quantities by replacing with 0
- Calculates total order value as `Quantity × Price`
- Converts date to proper datetime format
- Extracts `Month` and `Year` columns for time-based analysis

### 3. 🔍 **Validate**
- Checks for:
  - Missing values  
  - Duplicates  
  - Negative quantities or prices  
- Logs warnings if issues are found.

### 4. 💾 **Load**
- Exports the cleaned dataset into `cleaned_orders.csv`
- Ensures the output directory exists
- Logs load completion time and location

---

## 🧮 Example Data

**`orders.csv`**
| Order_ID | Customer_ID | Product | Quantity | Price | Order_Date |
|-----------|--------------|----------|-----------|--------|-------------|
| 101 | 1 | Laptop | 1 | 4000 | 2024-02-01 |
| 102 | 2 | Headphones | 2 | 300 | 2024-02-02 |
| 103 | 3 | Mouse | *None* | 80 | 2024-02-03 |

**`customers.csv`**
| Customer_ID | Customer_Name | Region |
|--------------|----------------|---------|
| 1 | Ali | KL |
| 2 | Siti | Penang |
| 3 | Ravi | Johor |

After processing, the output file `cleaned_orders.csv` includes additional columns:

| Order_ID | Customer_Name | Region | Quantity | Price | Total_Value | Month | Year |
|-----------|----------------|---------|-----------|--------|--------------|-------|------|
| 101 | Ali | KL | 1.0 | 4000 | 4000.0 | 2 | 2024 |
| 102 | Siti | Penang | 2.0 | 300 | 600.0 | 2 | 2024 |

---

## 🪵 Logging Example

The pipeline uses Python’s built-in `logging` module to track progress.

Example log output:
2025-11-04 10:00:00 - INFO - Starting data extraction...
2025-11-04 10:00:01 - INFO - ✅ Data extracted successfully.
2025-11-04 10:00:02 - INFO - 🔧 Starting data transformation...
2025-11-04 10:00:03 - INFO - ✅ Transformation completed. Final shape: (5, 9)
2025-11-04 10:00:03 - INFO - 🧪 Starting data validation...
2025-11-04 10:00:03 - INFO - ✅ Data validation passed — no major issues found.
2025-11-04 10:00:04 - INFO - 💾 Data successfully saved to cleaned_orders.csv
2025-11-04 10:00:04 - INFO - 🏁 ETL pipeline completed successfully!


---

## 🧰 Tech Stack

| Component | Description |
|------------|-------------|
| **Language** | Python 3.13 |
| **Libraries** | `pandas`, `logging`, `os`, `datetime`, `time` |
| **Data Source** | CSV files (Orders & Customers) |
| **Output Format** | Cleaned CSV |


