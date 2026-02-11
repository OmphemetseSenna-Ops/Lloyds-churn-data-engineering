import pandas as pd

# Compare sheet names and columns between original and extracted files
def validate_structure(original_sheets, extracted_sheets):
    print("\nValidating File Structure...")

    try:
        original_sheet_names = set(original_sheets.keys())
        extracted_sheet_names = set(extracted_sheets.keys())
        print("Original sheets:", original_sheet_names)
        print("Extracted sheets:", extracted_sheet_names)
    
        missing_in_extracted = set(original_sheets.keys()) - set(extracted_sheets.keys())
        extra_in_extracted = set(extracted_sheets.keys()) - set(original_sheets.keys())

        print("Missing sheets in extracted:", missing_in_extracted)
        print("Extra sheets in extracted:", extra_in_extracted)

        for sheet in original_sheets.keys():
            try:
                if sheet in extracted_sheets:
                    original_columns = set(original_sheets[sheet]["columns"])
                    extracted_columns = set(extracted_sheets[sheet]["columns"])
                    print(f"\nSheet '{sheet}':")
                    print(" - Missing columns:", original_columns - extracted_columns)
                    print(" - Extra columns:", extracted_columns - original_columns)
            except Exception as e:
                print(f"Error comparing  columns for sheet '{sheet}': {e}")
    except Exception as e:
        print(f"Error validating file structure: {e}")


# Validate row counts, duplicates, and mismatches between original data and extracted data
def validate_data(original_df, extracted_df):
    print("\nValidating data...")
    
    try:
        print(f"Row count: Original={len(original_df)}, Extracted={len(extracted_df)}")
    except Exception as e:
        print(f"Error calculating row count: {e}")

    try:
        print(f"Duplicates: Original={original_df.duplicated().sum()}, Extracted={extracted_df.duplicated().sum()}")
    except Exception as e:
        print(f"Error calculating duplicates: {e}")

    try:
        common_columns = [col for col in extracted_df.columns if col in original_df.columns]
        merged_common = original_df[common_columns].dropna(how="all")
        extracted_common = extracted_df[common_columns].dropna(how="all")

        diff = pd.concat([extracted_common, merged_common]).drop_duplicates(keep=False)

        if diff.empty:
            print("Extracted data matches original merged data.")
        else:
            print("Differences found:")
            print(diff.head(20))
    except Exception as e:
        print(f"Error comparing data: {e}")