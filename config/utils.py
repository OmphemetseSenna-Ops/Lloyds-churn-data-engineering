import pandas as pd

# Extract all sheets and their columns dynamically
def extract_sheets(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        sheet_info = {}
        for sheet in xls.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet)
            sheet_info[sheet] = {
                "columns": df.columns.tolist(),
                "rows": len(df),
                "data": df
            }
        return sheet_info
    except Exception as e:
        print(f"Error extracting sheets from {file_path}: {e}")
        return {}
