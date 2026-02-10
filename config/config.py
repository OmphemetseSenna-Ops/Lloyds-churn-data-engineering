import os
import sys
from dotenv import load_dotenv


def load_env():
    try:
        load_dotenv()
        print("Environment variables loaded.")
    except Exception as e:
        print(f"Error loading environment variables: {e}")
        sys.exit(1)


def get_env_variable(var_name):
    try:
        value = os.getenv(var_name)
        if not value:
            raise ValueError(f"Environment variable {var_name} is not set.")
        return value
    except Exception as e:
        print(f"Error retrieving {var_name}: {e}")
        sys.exit(1)


def get_file_path(env_var_name):
    load_env()
    try:
        file_path = get_env_variable(env_var_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        print(f"Found file for {env_var_name}: {file_path}")
        return file_path
    except Exception as e:
        print(f"Error with {env_var_name}: {e}")
        sys.exit(1)



def get_process_name():
    load_env()
    return get_env_variable("PROCESS_NAME")



def get_db_connection_string(): 
    load_env() 
    driver = get_env_variable("DB_DRIVER") 
    server = get_env_variable("DB_SERVER") 
    database = get_env_variable("DB_NAME") 
    authentication = os.getenv("DB_AUTH", "trusted").lower() 
    if authentication == "trusted": 
        return f"Driver={{{driver}}};Server={server};Database={database};Trusted_Connection=yes;" 
    else: 
        user = get_env_variable("DB_USER") 
        password = get_env_variable("DB_PASSWORD") 
        return f"Driver={{{driver}}};Server={server};Database={database};UID={user};PWD={password};"


def get_max_retries():
    load_env()
    try:
        max_retries = int(get_env_variable("MAX_RETRIES"))
        return max_retries
    except ValueError as e:
        print(f"Invalid MAX_RETRIES value: {e}")
        sys.exit(1)



def get_retry_delay():
    load_env()
    try:
        retry_delay = int(get_env_variable("RETRY_DELAY"))
        return retry_delay
    except ValueError as e:
        print(f"Invalid RETRY_DELAY value: {e}")
        sys.exit(1)