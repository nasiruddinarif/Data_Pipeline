# ***************************
#   IMPORT LIBRARIES
# ***************************
import pandas as pd
import datetime
import logging
import os
import time

# ***************************
#   EXTRACT FUNCTIONS
# ***************************

def extract(orders_path, customers_path):
    # Extract data from order and customer CSV.
    start_time = time.time()
    logging.info("Starting data extraction...")

    try:
        orders = pd.read_csv(orders_path)
        customers = pd.read_csv(customers_path)
        logging.info("✅ Data extracted successfully.")
        logging.info(f"Orders: {orders.shape[0]} rows | Customers: {customers.shape[0]} rows")
    except Exception as e:
        logging.error(f"❌ Extraction failed: {e}")
        raise e
    
    end_time = time.time()
    logging.info(f'Extraction completed in {round(end_time - start_time, 2)} seconds')
    return orders, customers

# ***************************
#   TRANSFORM FUNCTIONS
# ***************************
def transform(orders, customers):
    # Clean, merge and enrich the orders dataset.
    start_time = time.time()
    logging.info("🔧 Starting data transformation...")

    try:
        # Merge orders with customers
        merged_df = pd.merge(orders, customers, on="Customer_ID", how="left")

        # Fill missing quantities with 0
        merged_df["Quantity"].fillna(0, inplace=True)

        # Calculate total order value
        merged_df["Total_Value"] = merged_df["Quantity"] * merged_df["Price"]

        # Convert Order_Date to datetime and extract Month and Year
        merged_df["Order_Date"] = pd.to_datetime(merged_df["Order_Date"])
        merged_df["Month"] = merged_df["Order_Date"].dt.month
        merged_df["Year"] = merged_df["Order_Date"].dt.year

        logging.info(f"✅ Transformation completed. Final shape: {merged_df.shape}")
    except Exception as e:
        logging.error(f"❌ Transformation failed: {e}")
        raise e

    end_time = time.time()
    logging.info(f"Transformation completed in {round(end_time - start_time, 2)} seconds")
    return merged_df

# ***************************
#   VALIDATE FUNCTIONS
# ***************************

def validate(df):
    # Perform data quality checks.
    logging.info("🧪 Starting data validation...")

    try:
        issues_found = False

        # Check missing values
        missing = df.isnull().sum().sum()
        if missing > 0:
            logging.warning(f"⚠️ Found {missing} missing values.")
            issues_found = True

        # Check duplicates
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            logging.warning(f"⚠️ Found {duplicates} duplicate rows.")
            issues_found = True

        # Check invalid quantity or price
        invalid_qty = (df["Quantity"] < 0).sum()
        invalid_price = (df["Price"] < 0).sum()

        if invalid_qty > 0 or invalid_price > 0:
            logging.warning(f"⚠️ Found {invalid_qty} invalid Quantity and {invalid_price} invalid Price values.")
            issues_found = True

        if not issues_found:
            logging.info(f"✅ Data validation passed — no major issues found.")
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        raise e
    
# ***************************
#   LOAD FUNCTIONS
# ***************************

def load(clean_data, output_path):
    # Load validated data into new CSV file.
    start_time = time.time()
    logging.info(f"💾 Starting data loading...")
    

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        clean_data.to_csv(output_path, index=False)
        logging.info(f"✅ Data successfully saved to {output_path}")
    except Exception as e:
        logging.error(f"❌ Loading failed: {e}")
        raise e
    
    end_time = time.time()
    logging.info(f"Loading completed in {round(end_time - start_time, 2)} seconds")

# ***************************
#   MAIN PIPELINE EXECUTION
# ***************************

if __name__ == "__main__":
    orders_path = r"D:\data_engineer_project\ecommerce_order_etl_pipeline\orders.csv"
    customers_path = r"D:\data_engineer_project\ecommerce_order_etl_pipeline\customers.csv"
    output_path = r"D:\data_engineer_project\ecommerce_order_etl_pipeline\cleaned_orders.csv"

    # Extract
    orders_df, customers_df = extract(orders_path, customers_path)

    # Transform
    Transformed_df = transform(orders_df, customers_df)

    # Validate
    validate(Transformed_df)

    # Load
    load(Transformed_df, output_path)

    logging.info("🏁 ETL pipeline completed successfully!")
