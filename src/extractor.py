import os
import sys
import pandas as pd
from dotenv import load_dotenv



def extract_relevant_data():
    try:
        load_dotenv()
        input_file = os.getenv('DATA_FILE_PATH')
        
        if not input_file:
            raise ValueError("Environment variable DATA_FILE_PATH is not set.")
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"File not found: {input_file}")
        print(f"Input file found: {input_file}")
    except Exception as e:
        print(f"Error loading environment variables or file path: {e}")
        sys.exit(1)

    try:
        demographics = pd.read_excel(input_file, sheet_name="Customer_Demographics")
        transactions = pd.read_excel(input_file, sheet_name="Transaction_History")
        service = pd.read_excel(input_file, sheet_name="Customer_Service")
        churn = pd.read_excel(input_file, sheet_name="Churn_Status")
        print("All sheets loaded successfully.")
    except Exception as e:
        print(f"Error reading sheets: {e}")
        sys.exit(1)

    try:
        demographics = demographics[["CustomerID", "Age", "Gender", "MaritalStatus", "IncomeLevel"]]
        transactions = transactions[["CustomerID", "TransactionID", "TransactionDate", "AmountSpent", "ProductCategory"]]
        service = service[["CustomerID", "InteractionID", "InteractionDate", "InteractionType", "ResolutionStatus"]]
        churn = churn[["CustomerID", "ChurnStatus"]]
        print("Relevant columns selected.")
    except KeyError as e:
        print(f"Missing expected column: {e}")
        sys.exit(1)

    try:
        combined_data = (
            demographics
            .merge(transactions, on="CustomerID", how="left")
            .merge(service, on="CustomerID", how="left")
            .merge(churn, on="CustomerID", how="left")
        )
        print("Data merged successfully.")
    except Exception as e:
        print(f"Error merging data: {e}")
        sys.exit(1)

    try:
        output_file = os.getenv('RELEVANT_DATA_FILE_PATH', "Customer_Churn_Relevant_Data.xlsx")
        combined_data.to_excel(output_file, index=False)
        print("Customer churn dataset created using identified data elements only")
        print(f"Output file saved as: {output_file}")
        return output_file
    except Exception as e:
        print(f"Error saving output file: {e}")
        sys.exit(1)

