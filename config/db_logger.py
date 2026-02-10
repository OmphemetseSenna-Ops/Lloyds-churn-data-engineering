import pyodbc
import datetime
from config import config

def get_connection():
    conn_str = config.get_db_connection_string()
    return pyodbc.connect(conn_str)

def log_process(process_name, start_time, end_time, status, records_inserted, message):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO [ETL Process Log] 
            ([Process Name], [Start Time], [End Time], [Status], [Records Inserted], [Message])
            VALUES (?, ?, ?, ?, ?, ?)
        """, (process_name, start_time, end_time, status, records_inserted, message))
        conn.commit()
        cursor.close()
        conn.close()
        print("Process log saved to database.")
    except Exception as e:
        print(f"Error saving process log: {e}")

def log_error(process_name, error_step, error_message):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO [ETL Error Log] 
            ([Process Name], [Error Time], [Error Step], [Error Message])
            VALUES (?, ?, ?, ?)
        """, (process_name, datetime.datetime.now(), error_step, error_message))
        conn.commit()
        cursor.close()
        conn.close()
        print("Error log saved to database.")
    except Exception as e:
        print(f"Error saving error log: {e}")