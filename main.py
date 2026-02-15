import pandas as pd
import datetime
import time

from config import config, utils, db_logger
from src import extractor, validator, explore_data


MAX_RETRIES = config.get_max_retries()
RETRY_DELAY = config.get_retry_delay()

def run_extraction_batch():
    process_name = config.get_process_name()
    start_time = datetime.datetime.now() 
    attempt = 0 
    records_inserted = 0

    while attempt < MAX_RETRIES:
        try:
            print(f"Starting extraction batch for {process_name} (Attempt {attempt + 1})")
            output_file = extractor.extract_relevant_data()
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            records_inserted = 1
            print(f"Extraction batch completed successfully in {duration:.2f} seconds.")
            
            db_logger.log_process(process_name, start_time, end_time, "Success", records_inserted, f"Extraction succeeded on attempt {attempt}")
            return True, output_file
        
        except Exception as e:
            print(f"Error during extraction batch: {e}")
            attempt += 1
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                end_time = datetime.datetime.now()
                duration = (end_time - start_time).total_seconds()
                print(f"Extraction batch failed after {MAX_RETRIES} attempts. Total duration: {duration:.2f} seconds.")
                

                db_logger.log_process(process_name, start_time, end_time, "Failure", records_inserted, str(e))
                return False, None


def run_validation_batch(extracted_file, original_file): 
    process_name = config.get_process_name()
    start_time = datetime.datetime.now() 
    attempt = 0 
    records_validated = 0 
    while attempt < MAX_RETRIES: 
        attempt += 1 
        print(f"\nValidation attempt {attempt}...") 
        try: 
            original_sheets = utils.extract_sheets(original_file) 
            extracted_sheets = utils.extract_sheets(extracted_file) 
            merged_df = ( original_sheets["Customer_Demographics"]["data"] .merge(original_sheets["Transaction_History"]["data"], on="CustomerID", how="outer") .merge(original_sheets["Customer_Service"]["data"], on="CustomerID", how="outer") .merge(original_sheets["Churn_Status"]["data"], on="CustomerID", how="outer") ) 
            extracted_df = extracted_sheets["Sheet1"]["data"]
            validator.validate_structure(original_sheets, extracted_sheets)
            validator.validate_data(merged_df, extracted_df)
            end_time = datetime.datetime.now() 
            duration = (end_time - start_time).total_seconds() 
            records_validated = len(extracted_df) 
            print(f"Validation batch completed successfully in {duration:.2f} seconds.") 

            db_logger.log_process(process_name, start_time, end_time, "Success", records_validated, f"Validation succeeded on attempt {attempt}") 
            return True
        except Exception as e: 
            print(f"Error during validation batch: {e}") 
            if attempt < MAX_RETRIES: 
                print(f"Retrying validation in {RETRY_DELAY} seconds...") 
                time.sleep(RETRY_DELAY) 
            else: 
                end_time = datetime.datetime.now() 
                duration = (end_time - start_time).total_seconds() 
                print(f"Validation batch failed after {MAX_RETRIES} attempts. Total duration: {duration:.2f} seconds.") 

                db_logger.log_process(process_name, start_time, end_time, "Failure", records_validated, str(e))
                return False


def run_profiling_batch(extracted_data):
    process_name = config.get_process_name() + "_Profiling"
    start_time = datetime.datetime.now()
    attempt = 0
    records_profiled = 0

    while attempt < MAX_RETRIES:
        attempt += 1
        print(f"\nProfiling attempt {attempt}...")
        try:
            customer_churn_data = pd.read_excel(extracted_data)
            records_profiled = explore_data.explore_dataset(customer_churn_data)
            
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Profiling batch completed successfully in {duration:.2f} seconds.")

            db_logger.log_process(process_name, start_time, end_time, "Success", records_profiled, f"Profiling succeeded on attempt {attempt}")
            return True
        except Exception as e:
            print(f"Error during profiling batch: {e}")
            if attempt < MAX_RETRIES:
                print(f"Retrying profiling in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                end_time = datetime.datetime.now()
                duration = (end_time - start_time).total_seconds()
                print(f"Profiling batch failed after {MAX_RETRIES} attempts. Total duration: {duration:.2f} seconds.")

                db_logger.log_process(process_name, start_time, end_time, "Failure", records_profiled, str(e))
                return False


def main():
    print("Starting ETL process...")

    success, output_file = run_extraction_batch()
    if success:
        original_file = config.get_file_path("DATA_FILE_PATH")
        if run_validation_batch(output_file, original_file):
            if run_profiling_batch(output_file):
                print("ETL process completed successfully.")
            else:
                print("ETL process completed with profiling errors.")



if __name__ == "__main__":
    main()

