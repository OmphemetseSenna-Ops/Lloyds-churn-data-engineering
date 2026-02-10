import pandas as pd
import datetime
import time

from config import config, utils, db_logger
from src import extractor


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
            return output_file
        
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


def run_validation_batch():
    # to continue.....
        pass

def main():
    print("Starting ETL process...")
    run_extraction_batch()

if __name__ == "__main__":
    main()
