import os
import sys
import pandas as pd

from config import config, utils, db_logger


def explore_dataset(customer_churn_data):

    print("Understanding Data Structure...\n")

    # 1. Shape of the dataset
    rows, columns = customer_churn_data.shape
    total_cells = rows * columns
    print("Number of Rows:", rows)
    print("Number of Columns:", columns)
    print("Total Cells:", total_cells)

    print("\nColumn Names:")
    print(customer_churn_data.columns.tolist())



    # 2. Data types of each column
    print("\nData Types of Each Column:")
    print(customer_churn_data.dtypes)


    # 3. Missing values analysis
    # 3.1 Calculate total missing values in the dataset
    missing_sum = customer_churn_data.isna().sum()
    total_missing = missing_sum.sum()
    print(f"\nTotal number of missing values in the dataset: {total_missing}")


    # 3.2 Identify columns with the highest number of missing values
    missing_percentage = (missing_sum / len(customer_churn_data) * 100).round(2).sort_values(ascending=False)
    print("\nMissing Values in Each Column:\n", missing_sum)
    print("\nMissing Values Percentage (%):\n", missing_percentage)


    # 3.3 Identify columns with all values missing
    num_columns_all_missing = len(customer_churn_data.columns[customer_churn_data.isna().all()])
    if num_columns_all_missing > 0:
        missing_all_columns = customer_churn_data.columns[customer_churn_data.isna().all()]
        print(f"\nColumns with all values missing: {missing_all_columns.tolist()}")
    else:
        print("\nNo columns with all missing values.")


    # 3.4 Identify rows with all values missing
    num_rows_all_missing = len(customer_churn_data[customer_churn_data.isna().all(axis=1)])
    if num_rows_all_missing > 0:
        missing_all_rows = customer_churn_data[customer_churn_data.isna().all(axis=1)]
        print(f"\nNumber of rows with all values missing: {num_rows_all_missing}")
        print(missing_all_rows)
    else:
        print("\nNo rows with all values missing found.")





    # 4. DUPLICATE ROWS ANALYSIS
    # 4.1 Check for duplicate rows in the dataset
    duplicate_rows = customer_churn_data.duplicated()
    num_duplicate_rows = duplicate_rows.sum()
    print(f"\nNumber of duplicate rows in the dataset: {num_duplicate_rows}")

    # 4.2 Show the duplicate rows
    if num_duplicate_rows > 0:
        duplicate_rows_data = customer_churn_data[customer_churn_data.duplicated(keep=False)]
        print("\nDuplicate Rows:")
        print(duplicate_rows_data)
    else:
        print("\nNo duplicate rows found in the dataset.")

    # 4.3 Identify columns that contribute to duplicates
    if num_duplicate_rows > 0:
        duplicate_columns = customer_churn_data.columns[customer_churn_data.duplicated(keep=False)].tolist()
        print(f"\nColumns contributing to duplicates: {duplicate_columns}")
    else:
        print("\nNo columns contributing to duplicates since no duplicate rows found.")

    # 4.4 Show the number of duplicates for each unique value in the 'CustomerID' column
    if 'CustomerID' in customer_churn_data.columns:
        duplicate_counts = customer_churn_data['CustomerID'].value_counts()
        duplicate_counts = duplicate_counts[duplicate_counts > 1]
        print("\nNumber of duplicates for each unique CustomerID:")
        print(duplicate_counts)
    else:
        print("\n'CustomerID' column not found in the dataset, cannot analyze duplicates based on CustomerID.")


    # 4.5 Show the number of duplicates for each unique value in the 'TransactionID' column
    if 'TransactionID' in customer_churn_data.columns:
        duplicate_transaction_counts = customer_churn_data['TransactionID'].value_counts()
        duplicate_transaction_counts = duplicate_transaction_counts[duplicate_transaction_counts > 1]
        print("\nNumber of duplicates for each unique TransactionID:")
        print(duplicate_transaction_counts)
    else:
        print("\n'TransactionID' column not found in the dataset, cannot analyze duplicates based on TransactionID.")





    # 5. CARDINALITY CHECK
    # 5.1 Get the number of unique customers based on 'CustomerID'
    if 'CustomerID' in customer_churn_data.columns:
        unique_customers = customer_churn_data["CustomerID"].nunique()
        print(f"\nNumber of unique customers: {unique_customers}")
        if unique_customers > 0:
            print(f"Unique CustomerIDs:\n{customer_churn_data['CustomerID'].unique()}")
        else:
            print("\nNo unique customers found in the dataset.")
    else:
        print("\n'CustomerID' column not found in the dataset, cannot analyze unique customers.")



    # 5.2 In 'TransactionID' column, check for non-numeric values or values less than 0
    if 'TransactionID' in customer_churn_data.columns:
        non_numeric_transaction_ids = customer_churn_data[~customer_churn_data['TransactionID'].apply(lambda x: isinstance(x, (int, float)))]
        negative_transaction_ids = customer_churn_data[customer_churn_data['TransactionID'] < 0]
        num_non_numeric_transaction_ids = len(non_numeric_transaction_ids)
        num_negative_transaction_ids = len(negative_transaction_ids)
        print(f"\nNumber of non-numeric values in 'TransactionID' column: {num_non_numeric_transaction_ids}")
        print(f"Number of negative values in 'TransactionID' column: {num_negative_transaction_ids}")
        if num_non_numeric_transaction_ids > 0:
            print("Non-numeric values in 'TransactionID' column:")
            print(non_numeric_transaction_ids[['CustomerID', 'TransactionID']])
        if num_negative_transaction_ids > 0:
            print("Negative values in 'TransactionID' column:")
            print(negative_transaction_ids[['CustomerID', 'TransactionID']])
    else:
        print("\n'TransactionID' column not found in the dataset, cannot analyze non-numeric or negative values in 'TransactionID' column.")





    # 5.3 Get the number of unique products based on 'ProductCategory'
    if 'ProductCategory' in customer_churn_data.columns:
        unique_product_categories = customer_churn_data["ProductCategory"].nunique()
        print(f"\nNumber of unique product categories: {unique_product_categories}")
        if unique_product_categories > 0:
            print(f"Unique Product Categories:\n{customer_churn_data['ProductCategory'].unique()}")
        else:
            print("\nNo unique product categories found in the dataset.")
    else:
        print("\n'ProductCategory' column not found in the dataset, cannot analyze unique product categories.")



    # 5.4 Get the number of unique interaction types based on 'InteractionType'
    if 'InteractionType' in customer_churn_data.columns:
        unique_interaction_types = customer_churn_data["InteractionType"].nunique()
        print(f"\nNumber of unique interaction types: {unique_interaction_types}")
        if unique_interaction_types > 0:
            print(f"Unique Interaction Types:\n{customer_churn_data['InteractionType'].unique()}")
        else:
            print("\nNo unique interaction types found in the dataset.")
    else:
        print("\n'InteractionType' column not found in the dataset, cannot analyze unique interaction types.")




    # 5.5 Get the number of unique churn statuses based on 'ChurnStatus'
    if 'ChurnStatus' in customer_churn_data.columns:
        unique_churn_statuses = customer_churn_data["ChurnStatus"].nunique()
        print(f"\nNumber of unique churn statuses: {unique_churn_statuses}")
        if unique_churn_statuses > 0:
            print(f"Unique Churn Statuses:\n{customer_churn_data['ChurnStatus'].unique()}")
        else:
            print("\nNo unique churn statuses found in the dataset.")
    else:
        print("\n'ChurnStatus' column not found in the dataset, cannot analyze unique churn statuses.")




    # 5.6 Get the number of unique values in the 'Gender' column
    if 'Gender' in customer_churn_data.columns:
        unique_genders = customer_churn_data["Gender"].nunique()
        print(f"\nNumber of unique genders: {unique_genders}")
        if unique_genders > 0:
            print(f"Unique Genders:\n{customer_churn_data['Gender'].unique()}")
        else:
            print("\nNo unique genders found in the dataset.")
    else:
        print("\n'Gender' column not found in the dataset, cannot analyze unique genders.")





    # 5.7 Get the number of unique values in the 'MaritalStatus' column
    if 'MaritalStatus' in customer_churn_data.columns:
        unique_marital_statuses = customer_churn_data["MaritalStatus"].nunique()
        print(f"\nNumber of unique marital statuses: {unique_marital_statuses}")
        if unique_marital_statuses > 0:
            print(f"Unique Marital Statuses:\n{customer_churn_data['MaritalStatus'].unique()}")
        else:
            print("\nNo unique marital statuses found in the dataset.")
    else:
        print("\n'MaritalStatus' column not found in the dataset, cannot analyze unique marital statuses.")




    # 5.8 Get the number of unique values in the 'IncomeLevel' column
    if 'IncomeLevel' in customer_churn_data.columns:
        unique_income_levels = customer_churn_data["IncomeLevel"].nunique()
        print(f"\nNumber of unique income levels: {unique_income_levels}")
        if unique_income_levels > 0:
            print(f"Unique Income Levels:\n{customer_churn_data['IncomeLevel'].unique()}")
        else:
            print("\nNo unique income levels found in the dataset.")
    else:
        print("\n'IncomeLevel' column not found in the dataset, cannot analyze unique income levels.")




    # 5.9 In AmountSpent column, check for non-numeric values and values less than 0
    if 'AmountSpent' in customer_churn_data.columns:
        non_numeric_amounts = customer_churn_data[~customer_churn_data['AmountSpent'].apply(lambda x: isinstance(x, (int, float)))]
        negative_amounts = customer_churn_data[customer_churn_data['AmountSpent'] < 0]
        num_non_numeric_amounts = len(non_numeric_amounts)
        num_negative_amounts = len(negative_amounts)
        print(f"\nNumber of non-numeric values in 'AmountSpent' column: {num_non_numeric_amounts}")
        print(f"Number of negative values in 'AmountSpent' column: {num_negative_amounts}")
        if num_non_numeric_amounts > 0:
            print("Non-numeric values in 'AmountSpent' column:")
            print(non_numeric_amounts[['CustomerID', 'AmountSpent']])
        else:
            print("\nNo non-numeric values found in 'AmountSpent' column.")
        if num_negative_amounts > 0:
            print("Negative values in 'AmountSpent' column:")
            print(negative_amounts[['CustomerID', 'AmountSpent']])
        else:
            print("\nNo negative values found in 'AmountSpent' column.")
    else:
        print("\n'AmountSpent' column not found in the dataset, cannot analyze non-numeric or negative values in 'AmountSpent' column.")



    # 5.10 In 'Age' column, check for non-numeric values and values less than 0
    if 'Age' in customer_churn_data.columns:
        non_numeric_ages = customer_churn_data[~customer_churn_data['Age'].apply(lambda x: isinstance(x, (int, float)))]
        negative_ages = customer_churn_data[customer_churn_data['Age'] < 0]
        num_non_numeric_ages = len(non_numeric_ages)
        num_negative_ages = len(negative_ages)
        print(f"\nNumber of non-numeric values in 'Age' column: {num_non_numeric_ages}")
        print(f"Number of negative values in 'Age' column: {num_negative_ages}")
        if num_non_numeric_ages > 0:
            print("Non-numeric values in 'Age' column:")
            print(non_numeric_ages[['CustomerID', 'Age']])
        else:
            print("\nNo non-numeric values found in 'Age' column.")
        if num_negative_ages > 0:
            print("Negative values in 'Age' column:")
            print(negative_ages[['CustomerID', 'Age']])
        else:
            print("\nNo negative values found in 'Age' column.")




    # 5.11 Check if DateTime data have the same format in 'TransactionDate' and 'InteractionDate' columns
    if 'TransactionDate' in customer_churn_data.columns and 'InteractionDate' in customer_churn_data.columns:
        transaction_date_formats = customer_churn_data['TransactionDate'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notna(x) else None).unique()
        interaction_date_formats = customer_churn_data['InteractionDate'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notna(x) else None).unique()
        print(f"\nUnique date formats in 'TransactionDate' column: {transaction_date_formats}")
        print(f"Unique date formats in 'InteractionDate' column: {interaction_date_formats}")
        if len(transaction_date_formats) == 1 and len(interaction_date_formats) == 1 and transaction_date_formats[0] == interaction_date_formats[0]:
            print("\nDate formats in 'TransactionDate' and 'InteractionDate' columns are consistent.")
        else:
            print("\nDate formats in 'TransactionDate' and 'InteractionDate' columns are not consistent.")
    else:
        print("\n'TransactionDate' and/or 'InteractionDate' column not found in the dataset, cannot analyze date format consistency between 'TransactionDate' and 'InteractionDate' columns.")




    # 5.13 timestamps in 'TransactionDate' and 'InteractionDate' columns should not be in the future
    current_date = pd.to_datetime('today')
    if 'TransactionDate' in customer_churn_data.columns:
        future_transaction_dates = customer_churn_data[pd.to_datetime(customer_churn_data['TransactionDate'], errors='coerce') > current_date]
        num_future_transaction_dates = len(future_transaction_dates)
        print(f"\nNumber of future dates in 'TransactionDate' column: {num_future_transaction_dates}")
        if num_future_transaction_dates > 0:
            print("Future dates in 'TransactionDate' column:")
            print(future_transaction_dates[['CustomerID', 'TransactionDate']])
        else:
            print("\nNo future dates found in 'TransactionDate' column.")
    else:
        print("\n'TransactionDate' column not found in the dataset, cannot analyze future dates in 'TransactionDate' column.")















