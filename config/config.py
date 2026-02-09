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
