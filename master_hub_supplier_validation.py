# main.py

# Metadata comments (retain these as needed)
#log fixed
#mismatch fixed
#count in each heading
#Duplicate_table_fixed
#Big_numeric_readibiliy
#paragraph to heading
#ToC added
#format issue enchaned
#pincode issue added and enchanced
#enchanced scalabiltiy incase target_key changes acorrding to table
# working error log
#imp and non imp added
#4 columns made
#individual reports
#excel_try
#merging null tables
#merging duplicate  tables


import time
import os
import re
import logging
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from google.cloud.bigquery import SchemaField
import traceback
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
PROJECT_ID = 'fynd-jio-impetus-prod'       # Replace with your project ID
DATASET_ID = 'Impetus_dev_prod'                 # Replace with your dataset ID

# # Configuration
# PROJECT_ID = 'fynd-jio-impetus-non-prod'       # Replace with your project ID
# DATASET_ID = 'Impetus_dev_sit'                 # Replace with your dataset ID
PREFIXES = ['procuro_', 'costing_engine_', 'scan_pack_', 'pigeon_']  # Define your prefixes
# time_stamp,issue, error_message, tables_compared, issue_table, issue_column, unique_identifer
ERROR_LOG_M = []

# Get the current datetime
now = datetime.now()

Non_imp_columns = {
    'supplier': ['id', '_id', 'updated_at', 'created_at'],
    'vendor_details': ['id', '_id', 'updated_at', 'created_at']  # Add if applicable
}

Imp_columns = {}

# Mapping of base table names to their master key and target keys per prefix
BASE_TABLES = {

    'supplier': {
        'master_key': 'supplier_code',
        'targets': {
            'procuro_': 'supplier_code',
            'costing_engine_': 'supplier_code'
        },
        # 'active_filter': {  # Apply active filter
        #     'column': 'is_active',
        #     'value': True
        # },
        'perform_checks': True
    },
    'vendor_details': {  # Newly added entry
        'master_key': 'supplier_code',  # Using supplier_code as the key
        'master_table': 'master_hub_supplier',  # Specify the master table explicitly
        'targets': {
            'scan_pack_': 'vendor_code'
        },
        # 'active_filter': {  # Apply active filter
        #     'column': 'is_active',
        #     'value': True
        # },
        'perform_checks': True
    },
}

# Slack configuration
SLACK_TOKEN = "xoxb-2151238541-7946286860052-5FCcfqBPem0xKigGlIcKdLgX"
# SLACK_CHANNEL = "C07UN19ETK5"
SLACK_CHANNEL = "C08310RS2PK"

# Initialize Slack client
if SLACK_TOKEN and SLACK_CHANNEL:
    slack_client = WebClient(token=SLACK_TOKEN)
    logging.info("Slack client initialized successfully.")
else:
    slack_client = None
    logging.warning("Slack token or channel not found. Slack notifications will be disabled.")


def get_bigquery_client(project_id):
    """
    Initialize and return a BigQuery client.

    Args:
        project_id (str): GCP project ID.

    Returns:
        bigquery.Client: An initialized BigQuery client.
    """
    try:
        client = bigquery.Client(project=project_id)
        logging.info("BigQuery client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize BigQuery client: {e}")
        raise

def find_common_tables_with_master_hub(client, dataset_name, prefixes, base_tables):
    """
    Find tables in the specified dataset that share the same base name after removing the 'master_hub_' prefix
    and exist with other given prefixes.

    Args:
        client (bigquery.Client): Initialized BigQuery client.
        dataset_name (str): The name of the dataset to search within.
        prefixes (list): List of prefixes to compare with 'master_hub_'.
        base_tables (dict): The BASE_TABLES dictionary containing base table configurations.

    Returns:
        dict: A dictionary where keys are base names and values are dictionaries showing which prefixes have tables.
    """
    try:
        # Reference the dataset
        dataset_ref = client.dataset(dataset_name)

        # List all tables in the dataset
        tables = client.list_tables(dataset_ref)
        table_names = [table.table_id for table in tables]
        logging.info(f"Found {len(table_names)} tables in dataset '{dataset_name}'.")

        # Dictionary to hold base names and their corresponding tables
        common_tables = {}
        for base_name, config in base_tables.items():
            # Determine the master table
            master_table = config.get('master_table', f'master_hub_{base_name}')
            if master_table in table_names:
                common_tables[base_name] = {'master_hub_': master_table}
                # Check for target tables with specified prefixes
                for prefix, target_key in config.get('targets', {}).items():
                    target_table = f"{prefix}{base_name}"
                    if target_table in table_names:
                        common_tables[base_name][prefix] = target_table
            else:
                logging.warning(f"Master table '{master_table}' for base '{base_name}' not found in dataset.")

        # Filter out base names that only have 'master_hub_' but no other matching prefixes
        common_tables_with_prefixes = {base_name: tables for base_name, tables in common_tables.items() if len(tables) > 1}

        logging.info(f"Identified {len(common_tables_with_prefixes)} common base names with 'master_hub_' and other specified prefixes.")
        return common_tables_with_prefixes

    except GoogleAPIError as e:
        logging.error(f"Google API Error: {e.message}")
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return {}

def get_table_schema(client, dataset_name, table_name):
    """
    Retrieve the schema of a specified BigQuery table.

    Args:
        client (bigquery.Client): Initialized BigQuery client.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.

    Returns:
        dict: A dictionary mapping column names to their data types.
    """
    try:
        table_ref = client.dataset(dataset_name).table(table_name)
        table = client.get_table(table_ref)
        schema = {field.name: field.field_type for field in table.schema}
        logging.info(f"Retrieved schema for table '{table_name}'.")
        return schema
    except GoogleAPIError as e:
        logging.error(f"Failed to retrieve schema for table '{table_name}': {e.message}")
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred while retrieving schema for table '{table_name}': {e}")
        return {}

def load_table_from_bigquery(client, dataset_name, table_name):
    """
    Load a table from BigQuery into a Pandas DataFrame.

    Args:
        client (bigquery.Client): Initialized BigQuery client.
        dataset_name (str): The name of the dataset.
        table_name (str): The name of the table.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    try:
        query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`"
        df = client.query(query).to_dataframe()
        logging.info(f"Loaded data from table '{table_name}' into DataFrame.")
        return df
    except GoogleAPIError as e:
        logging.error(f"Failed to load table '{table_name}': {e.message}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading table '{table_name}': {e}")
        return pd.DataFrame()

def find_common_and_non_common_columns(df1, df2):
    """
    Identify common and unique columns between two DataFrames.

    Args:
        df1 (pd.DataFrame): First DataFrame.
        df2 (pd.DataFrame): Second DataFrame.

    Returns:
        tuple: (common_columns, df1_unique_columns, df2_unique_columns)
    """
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    df1_unique_columns = list(set(df1.columns) - set(df2.columns))
    df2_unique_columns = list(set(df2.columns) - set(df1.columns))
    logging.info(f"Found {len(common_columns)} common columns, {len(df1_unique_columns)} unique to first table, {len(df2_unique_columns)} unique to second table.")
    return common_columns, df1_unique_columns, df2_unique_columns

def find_mismatches(df_master, df_target, common_columns, master_key, target_key, table1, table2, duplicates_master, duplicates_target, non_imp_columns):
    """
    Identify mismatches between two DataFrames based on common columns and key columns, with case-insensitive comparisons for string columns.

    Args:
        df_master (pd.DataFrame): Source DataFrame (master_hub_ table).
        df_target (pd.DataFrame): Target DataFrame (prefixed table).
        common_columns (list): List of common columns to compare.
        master_key (str): The key column in the master table.
        target_key (str): The key column in the target table.
        table1 (str): Name of the source table.
        table2 (str): Name of the target table.
        duplicates_master (pd.DataFrame): Duplicate keys in master table.
        duplicates_target (pd.DataFrame): Duplicate keys in target table.
        non_imp_columns (list): Columns to exclude from mismatch comparison.

    Returns:
        tuple: (List of mismatches, List of error logs)
    """
    mismatches = []
    error_logs_m = []
    now = datetime.now()  # Initialize the current timestamp

    # Ensure key columns are present in both DataFrames
    if master_key not in df_master.columns or target_key not in df_target.columns:
        logging.error(f"Key columns '{master_key}' or '{target_key}' not found in the respective tables.")
        return mismatches, error_logs_m

    # Rename target key to match master key for easier comparison
    df_target_renamed = df_target.rename(columns={target_key: master_key})

    # Drop duplicates based on the master_key to ensure unique keys for merging
    df_master_unique = df_master.drop_duplicates(subset=master_key)
    df_target_unique = df_target_renamed.drop_duplicates(subset=master_key)

    # Merge DataFrames on the master_key
    merged_df = pd.merge(
        df_master_unique,
        df_target_unique,
        on=master_key,
        suffixes=(f'_{table1}', f'_{table2}'),
        how='inner'
    )

    logging.info(f"Merged DataFrame has {len(merged_df)} records for mismatch comparison.")

    for index, row in merged_df.iterrows():
        key = row[master_key]
        for column in common_columns:
            if column.startswith('_boltic_') or column in non_imp_columns:
                continue  # Skip columns starting with '_boltic_' or non-important columns

            master_col = f"{column}_{table1}"
            target_col = f"{column}_{table2}"

            val_master = row.get(master_col)
            val_target = row.get(target_col)

            # Convert string values to lowercase for case-insensitive comparison
            if isinstance(val_master, str):
                val_master_cmp = val_master.lower()
            else:
                val_master_cmp = val_master

            if isinstance(val_target, str):
                val_target_cmp = val_target.lower()
            else:
                val_target_cmp = val_target

            # Handle NaN values in comparison
            if pd.isna(val_master_cmp) and pd.isna(val_target_cmp):
                continue  # Both are NaN, treat as equal
            elif pd.isna(val_master_cmp) or pd.isna(val_target_cmp) or val_master_cmp != val_target_cmp:
                mismatch_detail = {
                    master_key: key,
                    'column': column,
                    f'{table1}_value': val_master,
                    f'{table2}_value': val_target
                }
                mismatches.append(mismatch_detail)
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'mismatch',
                    'error_message': f'There is a mismatch in the value of the {column} of {table1} and {table2}',
                    'source_table': table1,
                    'target_table': table2,
                    'issue_column': column,
                    'unique_identifier': f'{master_key}: {key}'
                }
                error_logs_m.append(error_detail)

    logging.info(f"Found {len(mismatches)} mismatches between '{table1}' and '{table2}'.")
    return mismatches, error_logs_m

def find_duplicates(df, key_column, table_name):
    """
    Detect duplicate key_column entries in the DataFrame and identify differences,
    ignoring specified columns.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        key_column (str): The key column to check for duplicates.
        table_name (str): Name of the table being checked.

    Returns:
        tuple: 
            - pd.DataFrame containing duplicated key_column values with differences.
            - list of error log dictionaries.
    """
    if key_column not in df.columns:
        logging.error(f"Key column '{key_column}' not found in DataFrame.")
        return pd.DataFrame(), []
    
    # Define columns to ignore when checking for differences
    ignore_columns = {'id', '_id', 'updated_at'}
    
    # Get all duplicate entries (keep=False to get all duplicates)
    duplicates_df = df[df.duplicated(subset=key_column, keep=False)]
    
    # Group by key_column
    grouped = duplicates_df.groupby(key_column)
    
    duplicate_records = []
    error_logs_m = []
    now = datetime.now()
    
    for key, group in grouped:
        if len(group) <= 1:
            continue  # Not a duplicate
        
        # Drop key_column and any columns starting with '_boltic_'
        non_boltic_cols = [col for col in group.columns if not col.startswith('_boltic_')]
        group_non_key = group[non_boltic_cols].drop(columns=[key_column])
        
        # Determine columns to check by excluding ignore_columns
        columns_to_check = [col for col in group_non_key.columns if col not in ignore_columns]
        
        if not columns_to_check:
            # If there are no columns to check after ignoring, treat as no difference
            difference = "No difference exists (only ignored columns differ)"
        else:
            # Check if all rows are identical in the columns_to_check
            subset = group_non_key[columns_to_check]
            if subset.nunique().sum() == 0:
                difference = "No difference exists"
            else:
                # Find which columns have differences
                cols_with_diff = subset.columns[subset.nunique() > 1].tolist()
                difference = "Difference in value of columns: " + ', '.join(cols_with_diff)
        
        # Only add to duplicate_records if differences exist outside ignored columns
        if difference != "No difference exists" and difference != "No difference exists (only ignored columns differ)":
            duplicate_records.append({
                key_column: key,
                'Difference in value': difference
            })
            error_detail = {
                'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                'issue': 'duplicate',
                'error_message': difference,
                'source_table': table_name,
                'target_table': '',
                'issue_column': '',
                'unique_identifier': f'{key_column}: {key}'
            }
            error_logs_m.append(error_detail)
    
    logging.info(f"Found {len(duplicate_records)} duplicate entries based on '{key_column}'.")
    return pd.DataFrame(duplicate_records), error_logs_m

def validate_data_types(schema_master, schema_target, master_key, table1_name, table2_name, columns_to_check):
    """
    Compare data types of common columns between master and target schemas.

    Args:
        schema_master (dict): Schema of the master table.
        schema_target (dict): Schema of the target table.
        master_key (str): The key column for reference.
        table1_name (str): Name of the first table.
        table2_name (str): Name of the second table.

    Returns:
        tuple:
            - pd.DataFrame containing data type discrepancies with table names in headers.
            - list of error log dictionaries.
    """
    data_type_issues = []
    error_logs_m = []

    # Identify common columns to check
    common_cols = set(columns_to_check).intersection(set(schema_master.keys()), set(schema_target.keys()))

    for column in common_cols:
        type_master = schema_master[column]
        type_target = schema_target[column]
        if type_master != type_target:
            data_type_issues.append({
                'column_name': column,
                f'{table1_name}_data_type': type_master,
                f'{table2_name}_data_type': type_target
            })
            error_detail = {
                'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                'issue': 'data_type_issues',
                'error_message': f'{table1_name}_data_type: {type_master} , {table2_name}_data_type: {type_target}',
                'source_table': f'{table1_name}',
                'target_table': f'{table2_name}',
                'issue_column': column,
                'unique_identifier': ''
            }
            error_logs_m.append(error_detail)

    logging.info(f"Found {len(data_type_issues)} data type issues.")
    return pd.DataFrame(data_type_issues), error_logs_m

def validate_formats(df_master, df_target, key_column, target_key, target_table, master_table):
    """
    Validate specific column formats using regular expressions and include corresponding target table values.

    Args:
        df_master (pd.DataFrame): The master DataFrame to validate.
        df_target (pd.DataFrame): The target DataFrame to fetch corresponding values.
        key_column (str): The key column in the master DataFrame.
        target_key (str): The key column in the target DataFrame.
        target_table (str): The name of the target table.
        master_table (str): The name of the master table.

    Returns:
        tuple:
            - pd.DataFrame containing format issues with corresponding target table values.
            - list of error log dictionaries.
    """
    format_issues = pd.DataFrame(columns=[key_column, 'column', 'value', 'issue', f'{target_table}_value'])
    error_logs_m = []
    now = datetime.now()

    for idx, row in df_master.iterrows():
        key_value = str(row[key_column]).strip()

        # GSTIN format validation
        if 'gstin' in df_master.columns:
            gstin = str(row['gstin']).strip()
            if not re.match(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[A-Z0-9]{3}$', gstin):
                # Fetch corresponding target value
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row['gstin'] if 'gstin' in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"

                format_issues = pd.concat([format_issues, pd.DataFrame([{
                    key_column: key_value,
                    'column': 'gstin',
                    'value': row['gstin'],
                    'issue': 'Invalid GSTIN format',
                    f'{target_table}_value': target_value
                }])], ignore_index=True)
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'format_issue',
                    'error_message': 'Invalid GSTIN format',
                    'source_table': f'{master_table}',
                    'target_table': '',
                    'issue_column': 'gstin',
                    'unique_identifier': f'{key_column}: {key_value}'
                }
                error_logs_m.append(error_detail)

        # Email format validation
        if 'email' in df_master.columns:
            email = str(row['email']).strip()
            if not re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', email):
                # Fetch corresponding target value
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row['email'] if 'email' in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"

                format_issues = pd.concat([format_issues, pd.DataFrame([{
                    key_column: key_value,
                    'column': 'email',
                    'value': row['email'],
                    'issue': 'Invalid email format',
                    f'{target_table}_value': target_value
                }])], ignore_index=True)
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'format_issue',
                    'error_message': 'Invalid email format',
                    'source_table': f'{master_table}',
                    'target_table': '',
                    'issue_column': 'email',
                    'unique_identifier': f'{key_column}: {key_value}'
                }
                error_logs_m.append(error_detail)

        # Pincode format validation
        if 'pincode' in df_master.columns:
            pincode = str(row['pincode']).strip()
            if not re.match(r'^\d{6}$', pincode):
                # Fetch corresponding target value
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row['pincode'] if 'pincode' in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"

                format_issues = pd.concat([format_issues, pd.DataFrame([{
                    key_column: key_value,
                    'column': 'pincode',
                    'value': row['pincode'],
                    'issue': 'Pincode must be exactly 6 digits',
                    f'{target_table}_value': target_value
                }])], ignore_index=True)
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'format_issue',
                    'error_message': 'Pincode must be exactly 6 digits',
                    'source_table': f'{master_table}',
                    'target_table': '',
                    'issue_column': 'pincode',
                    'unique_identifier': f'{key_column}: {key_value}'
                }

                error_logs_m.append(error_detail)

        # Address length validation
        if 'address' in df_master.columns:
            address = str(row['address']).strip()
            if len(address) > 100:
                # Fetch corresponding target value
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row['address'] if 'address' in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"

                format_issues = pd.concat([format_issues, pd.DataFrame([{
                    key_column: key_value,
                    'column': 'address',
                    'value': address,
                    'issue': 'Address exceeds 100 characters after stripping',
                    f'{target_table}_value': target_value
                }])], ignore_index=True)
                error_detail = {
                    'time_stamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'format_issue',
                    'error_message': 'Address exceeds 100 characters',
                    'source_table': master_table,
                    'target_table': '',
                    'issue_column': 'address',
                    'unique_identifier': f'{key_column}: {key_value}'
                }
                error_logs_m.append(error_detail)


        # PAN format validation
        if 'pan' in df_master.columns:
            pan = str(row['pan']).strip().upper()
            if not re.match(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$', pan):
                # Fetch corresponding target value
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row['pan'] if 'pan' in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"

                format_issues = pd.concat([format_issues, pd.DataFrame([{
                    key_column: key_value,
                    'column': 'pan',
                    'value': row['pan'],
                    'issue': 'Invalid PAN format',
                    f'{target_table}_value': target_value
                }])], ignore_index=True)
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'format_issue',
                    'error_message': 'Invalid PAN format',
                    'source_table': master_table,
                    'target_table': '',
                    'issue_column': 'pan',
                    'unique_identifier': f'{key_column}: {key_value}'
                }
                error_logs_m.append(error_detail)


    logging.info(f"Found {len(format_issues)} format issues.")
    return format_issues, error_logs_m


def make_df_excel_compatible(df):
    """
    Converts all columns in the DataFrame to string to ensure compatibility with Excel.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
    
    Returns:
        pd.DataFrame: The processed DataFrame with all data as strings.
    """
    try:
        # Convert all columns to string type
        df = df.astype(str)
        logging.info("Converted entire DataFrame to string type for Excel compatibility.")
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to string: {e}")
    
    return df



def create_excel_report(comparison_result, output_path):
    """
    Creates an Excel report with multiple sheets for different comparison results.

    Args:
        comparison_result (dict): A dictionary containing comparison results.
        output_path (str): The file path where the Excel report will be saved.

    Returns:
        None
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            sheets_created = False  # Flag to ensure at least one sheet is created

            # Function to safely create sheet names within Excel's 31 character limit
            def get_safe_sheet_name(name):
                return name if len(name) <= 31 else name[:28] + '...'

            # Pincode Mapping Issues
            if not comparison_result['pincode_mapping_issues'].empty:
                df = make_df_excel_compatible(comparison_result['pincode_mapping_issues'])
                sheet_name = get_safe_sheet_name(f'Pincode Mapping Issues in {comparison_result["table1_name"]}')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True

            # Mismatches
            if not comparison_result['mismatches'].empty:
                df = make_df_excel_compatible(comparison_result['mismatches'])
                sheet_name = get_safe_sheet_name('Mismatches in values')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True

            # Non-Matching Keys
            master_only_keys = comparison_result.get('df_master_only_keys', [])
            target_only_keys = comparison_result.get('df_target_only_keys', [])

            if master_only_keys or target_only_keys:
                # Create DataFrames with appropriate columns
                if master_only_keys:
                    df_master_only = pd.DataFrame(master_only_keys)
                    # Use actual table names instead of 'Master'
                    df_master_only.columns = [f"{comparison_result['key_column_master']} Only in {comparison_result['table1_name']}"]
                else:
                    # Create an empty DataFrame with the desired column name
                    df_master_only = pd.DataFrame(columns=[f"{comparison_result['key_column_master']} Only in {comparison_result['table1_name']}"])

                if target_only_keys:
                    df_target_only = pd.DataFrame(target_only_keys)
                    # Use actual table names instead of 'Target'
                    df_target_only.columns = [f"{comparison_result['key_column_target']} Only in {comparison_result['table2_name']}"]
                else:
                    # Create an empty DataFrame with the desired column name
                    df_target_only = pd.DataFrame(columns=[f"{comparison_result['key_column_target']} Only in {comparison_result['table2_name']}"])

                # Determine the maximum length between the two DataFrames
                max_len = max(len(df_master_only), len(df_target_only))

                # Reindex to ensure both DataFrames have the same number of rows
                df_master_only = df_master_only.reindex(range(max_len))
                df_target_only = df_target_only.reindex(range(max_len))

                # Concatenate side by side
                combined_keys_df = pd.concat([df_master_only, df_target_only], axis=1)

                # Make timezone-naive if any datetime columns
                combined_keys_df = make_df_excel_compatible(combined_keys_df)

                # Write to a single sheet
                sheet_name = get_safe_sheet_name('Non-Matching Keys')
                combined_keys_df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True
            else:
                pass  # No action needed if there are no non-matching keys

            # # Null values in master table
            # if not comparison_result['null_values_master'].empty:
            #     df = make_df_excel_compatible(comparison_result['null_values_master'])
            #     sheet_name = get_safe_sheet_name(f'Nulls in {comparison_result["table1_name"]}')
            #     df.to_excel(writer, sheet_name=sheet_name, index=False)
            #     sheets_created = True

            # # Null values in target table
            # if not comparison_result['null_values_target'].empty:
            #     df = make_df_excel_compatible(comparison_result['null_values_target'])
            #     sheet_name = get_safe_sheet_name(f'Nulls in {comparison_result["table2_name"]}')
            #     df.to_excel(writer, sheet_name=sheet_name, index=False)
            #     sheets_created = True

            # Consolidate Null Values from both master and target tables
            null_master = comparison_result.get('null_values_master', pd.DataFrame())
            null_target = comparison_result.get('null_values_target', pd.DataFrame())

            if not null_master.empty or not null_target.empty:
                # Add a 'table' column to identify the source
                if not null_master.empty:
                    null_master['table'] = comparison_result['table1_name']
                if not null_target.empty:
                    null_target['table'] = comparison_result['table2_name']

                # Combine both null DataFrames
                combined_nulls = pd.concat([null_master, null_target], ignore_index=True)

                # Group by 'supplier_code' and 'table' and aggregate 'column' values
                aggregated_nulls = combined_nulls.groupby(['supplier_code', 'table'])['column'] \
                    .apply(lambda cols: ', '.join(cols)).reset_index()

                # Rename columns for clarity
                aggregated_nulls.rename(columns={'column': 'columns_with_nulls'}, inplace=True)

                # Make DataFrame Excel compatible
                aggregated_nulls = make_df_excel_compatible(aggregated_nulls)

                # Write to a single sheet named 'Null Values'
                sheet_name = get_safe_sheet_name('Null Values')
                aggregated_nulls.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True


            # # Duplicate keys in master table
            # if not comparison_result['duplicates_master'].empty:
            #     df = make_df_excel_compatible(comparison_result['duplicates_master'])
            #     sheet_name = get_safe_sheet_name(f'Duplicates in {comparison_result["table1_name"]}')
            #     df.to_excel(writer, sheet_name=sheet_name, index=False)
            #     sheets_created = True

            # # Duplicate keys in target table
            # if not comparison_result['duplicates_target'].empty:
            #     df = make_df_excel_compatible(comparison_result['duplicates_target'])
            #     sheet_name = get_safe_sheet_name(f'Duplicates in {comparison_result["table2_name"]}')
            #     df.to_excel(writer, sheet_name=sheet_name, index=False)
            #     sheets_created = True

            # Consolidate Duplicate Values from both master and target tables
            duplicates_master = comparison_result.get('duplicates_master', pd.DataFrame())
            duplicates_target = comparison_result.get('duplicates_target', pd.DataFrame())

            if not duplicates_master.empty or not duplicates_target.empty:
                # Add a 'table' column to identify the source
                if not duplicates_master.empty:
                    duplicates_master['table'] = comparison_result['table1_name']
                if not duplicates_target.empty:
                    duplicates_target['table'] = comparison_result['table2_name']

                # Combine both duplicates DataFrames
                combined_duplicates = pd.concat([duplicates_master, duplicates_target], ignore_index=True)

                # Make DataFrame Excel compatible
                combined_duplicates = make_df_excel_compatible(combined_duplicates)

                # Rename columns for clarity
                combined_duplicates.rename(columns={'Difference in value': 'difference_in_value'}, inplace=True)

                # Write to a single sheet named 'Duplicate Values'
                sheet_name = get_safe_sheet_name('Duplicate Values')
                combined_duplicates.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True
            
            # Data type issues
            if not comparison_result['data_type_issues'].empty:
                df = make_df_excel_compatible(comparison_result['data_type_issues'])
                sheet_name = get_safe_sheet_name('Data Type Issues')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True

            # Format issues in master table
            if not comparison_result['format_issues_master'].empty:
                df = make_df_excel_compatible(comparison_result['format_issues_master'])
                sheet_name = get_safe_sheet_name(f'Format Issues in {comparison_result["table1_name"]}')
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_created = True

            # Ensure at least one sheet is present
            if not sheets_created:
                df_empty = pd.DataFrame({'Info': ['No data available for the comparison report.']})
                df_empty = make_df_excel_compatible(df_empty)  # Not strictly necessary, but for consistency
                df_empty.to_excel(writer, sheet_name='Report', index=False)

        logging.info(f"Saved Excel comparison report as '{output_path}'.")
    except Exception as e:
        logging.error(f"Failed to create Excel report: {e}")


def send_slack_alert(message):
    """
    Send a message to a specified Slack channel.

    Args:
        message (str): The message to send.
    """
    if not slack_client:
        logging.warning("Slack client is not initialized. Skipping Slack notification.")
        return

    try:
        response = slack_client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=message
        )
        logging.info(f"Message sent to {SLACK_CHANNEL}: {response['ts']}")
    except SlackApiError as e:
        logging.error(f"Error sending message to Slack: {e.response['error']}")

def upload_file_to_slack(filepath, title=None):
    """
    Upload a file to the specified Slack channel using files_upload_v2.

    Args:
        filepath (str): The path to the file to upload.
        title (str, optional): The title for the uploaded file. Defaults to the file's basename.
    """
    if not slack_client:
        logging.warning("Slack client is not initialized. Skipping file upload.")
        return

    try:
        with open(filepath, 'rb') as f:
            response = slack_client.files_upload_v2(
                channel=SLACK_CHANNEL,
                file=f,
                filename=os.path.basename(filepath),  # Explicitly set the filename with extension
                title=title if title else os.path.basename(filepath),  # Set the title
                initial_comment=title if title else "File uploaded."  # Optional: Add an initial comment
            )

        # Verify if the upload was successful
        if response.get('ok'):
            file_permalink = response['file']['permalink']
            logging.info(f"File uploaded to Slack channel '{SLACK_CHANNEL}': {file_permalink}")
        else:
            logging.error(f"Failed to upload file to Slack: {response}")
    except SlackApiError as e:
        logging.error(f"Slack API Error during file upload: {e.response['error']}")
    except Exception as e:
        logging.error(f"Unexpected error during file upload: {e}")

def find_non_matching_keys(df_master, df_target, master_key, target_key, duplicates_master, duplicates_target, master_table, target_table):
    """
    Identify keys present in df_master but not in df_target and vice versa, including duplicates.

    Args:
        df_master (pd.DataFrame): Source DataFrame.
        df_target (pd.DataFrame): Target DataFrame.
        master_key (str): The key column in the master table.
        target_key (str): The key column in the target table.
        duplicates_master (pd.DataFrame): Duplicate keys in master table.
        duplicates_target (pd.DataFrame): Duplicate keys in target table.
        master_table (str): Name of the master table.
        target_table (str): Name of the target table.

    Returns:
        tuple: (master_only_keys, target_only_keys, error_logs)
    """
    error_logs_m = []
    # Include all keys, including duplicates
    keys_master = set(df_master[master_key].astype(str).str.strip())
    keys_target = set(df_target[target_key].astype(str).str.strip())

    # Keys present only in master
    master_only = keys_master - keys_target
    # Keys present only in target
    target_only = keys_target - keys_master

    logging.info(f"Found {len(master_only)} keys in source not in target and {len(target_only)} keys in target not in source.")

    # Convert to list of dictionaries for consistency
    master_only_keys = [{master_key: key} for key in master_only]
    target_only_keys = [{target_key: key} for key in target_only]

    # Log errors for keys only in master
    for key in master_only:
        error_detail = {
            'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'issue': 'missing_key',
            'error_message': f"Key '{master_key}' with value '{key}' is present only in '{master_table}' and missing in '{target_table}'.",
            'source_table': master_table,
            'target_table': target_table, 
            'issue_column': master_key,
            'unique_identifier': f"{master_key}: {key}"
        }
        error_logs_m.append(error_detail)

    # Log errors for keys only in target
    for key in target_only:
        error_detail = {
            'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'issue': 'missing_key',
            'error_message': f"Key '{target_key}' with value '{key}' is present only in '{target_table}' and missing in '{master_table}'.",
            'source_table': target_table,
            'target_table': master_table,
            'issue_column': target_key,
            'unique_identifier': f"{target_key}: {key}"
        }
        error_logs_m.append(error_detail)

    return master_only_keys, target_only_keys, error_logs_m

def find_detailed_nulls(df_master, df_target, master_key, target_key, master_table, target_table, columns_to_check):
    """
    Identify null values in both master and target tables and fetch corresponding values or indicate missing keys.

    Args:
        df_master (pd.DataFrame): Source DataFrame (master_hub_ table).
        df_target (pd.DataFrame): Target DataFrame (prefixed table).
        master_key (str): The key column in the master table.
        target_key (str): The key column in the target table.
        master_table (str): Name of the master table.
        target_table (str): Name of the target table.

    Returns:
        tuple: (null_values_master, null_values_target, error_logs)
    """
    null_values_master = []
    null_values_target = []
    error_logs_m = []

    # Find nulls in master
    null_master = df_master[df_master[columns_to_check].isnull().any(axis=1)]
    for idx, row in null_master.iterrows():
        key_value = str(row[master_key]).strip()
        for column in df_master.columns:
            if column == master_key or column.startswith('_boltic_'):
                continue  # Skip key column and columns starting with '_boltic_'
            if pd.isnull(row[column]):
                if key_value in df_target[target_key].astype(str).str.strip().values:
                    target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value].iloc[0]
                    target_value = target_row[column] if column in target_row else "Column not present"
                else:
                    target_value = f"'{target_key}' not present"
                null_record = {
                    master_key: key_value,
                    'column': column,
                    target_table: target_value
                }
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'null',
                    'error_message': 'Null in columns',
                    'source_table': f'{master_table}',
                    'target_table': '',
                    'issue_column': column,
                    'unique_identifier': f'{master_key} : {key_value}'
                }
                error_logs_m.append(error_detail)
                null_values_master.append(null_record)

    # Find nulls in target
    null_target = df_target[df_target[columns_to_check].isnull().any(axis=1)]
    for idx, row in null_target.iterrows():
        key_value = str(row[target_key]).strip()
        for column in df_target.columns:
            if column == target_key or column.startswith('_boltic_'):
                continue  # Skip key column and columns starting with '_boltic_'
            if pd.isnull(row[column]):
                if key_value in df_master[master_key].astype(str).str.strip().values:
                    master_row = df_master[df_master[master_key].astype(str).str.strip() == key_value].iloc[0]
                    master_value = master_row[column] if column in master_row else "Column not present"
                else:
                    master_value = f"'{master_key}' not present"
                null_record = {
                    target_key: key_value,
                    'column': column,
                    master_table: master_value
                }
                error_detail = {
                    'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                    'issue': 'null',
                    'error_message': 'Null in columns',
                    'source_table': f'{target_table}',
                    'target_table': '',
                    'issue_column': column,
                    'unique_identifier': f'{target_key}: {key_value}'
                }
                error_logs_m.append(error_detail)
                null_values_target.append(null_record)

    logging.info(f"Found {len(null_values_master)} null values in master table '{master_table}'.")
    logging.info(f"Found {len(null_values_target)} null values in target table '{target_table}'.")
    return null_values_master, null_values_target, error_logs_m

def validate_pincode_mapping(df_master, df_target, key_column, target_key, target_table, client, master_table):
    """
    Validate pincode mapping by comparing with the all_india_PO_list reference table.
    If a pincode issue is found in the master table, then check the corresponding pincode in the target table.

    Args:
        df_master (pd.DataFrame): The master DataFrame to validate.
        df_target (pd.DataFrame): The target DataFrame to fetch corresponding values.
        key_column (str): The key column in the master DataFrame.
        target_key (str): The key column in the target DataFrame.
        target_table (str): The name of the target table.
        client (bigquery.Client): Initialized BigQuery client.
        master_table (str): Name of the master table.

    Returns:
        tuple:
            - pd.DataFrame: DataFrame containing pincode mapping issues with corresponding target table details.
            - list: List of error log dictionaries.
    """

    error_logs_m = []
    # Read the reference table from Analytics dataset
    try:
        reference_table = "all_india_po_list"
        reference_dataset = "analytics_data"
        # reference_table = "all_india_PO_list"
        # reference_dataset = "Analytics"
        query = f"SELECT pincode, city, state FROM `{PROJECT_ID}.{reference_dataset}.{reference_table}`"
        reference_df = client.query(query).to_dataframe()
        reference_df['pincode'] = reference_df['pincode'].astype(str).str.strip()
        reference_df['city'] = reference_df['city'].astype(str).str.strip().str.lower()
        reference_df['state'] = reference_df['state'].astype(str).str.strip().str.lower()
        logging.info(f"Loaded reference pincode mapping from '{reference_table}' in '{reference_dataset}' dataset.")
    except Exception as e:
        logging.error(f"Failed to load reference pincode mapping: {e}")
        return pd.DataFrame(), error_logs_m

    # Check if df_master has 'pincode', 'city', 'state' columns
    required_columns = {'pincode', 'city', 'state'}
    if not required_columns.issubset(df_master.columns):
        logging.info(f"DataFrame does not have required columns for pincode mapping validation: {required_columns}")
        return pd.DataFrame(), error_logs_m

    # Initialize the issues DataFrame with a single target table details column
    pincode_mapping_issues = pd.DataFrame(columns=[
        key_column, 'pincode', 'state', 'city', 'issue',
        f'{target_table}_details'
    ])

    # Iterate over each row in df_master to validate pincode mapping
    for idx, row in df_master.iterrows():
        key_value = str(row[key_column]).strip()
        pincode = str(row['pincode']).strip()
        city = str(row['city']).strip().lower()
        state = str(row['state']).strip().lower()

        # Fetch corresponding target row if exists
        target_row = df_target[df_target[target_key].astype(str).str.strip() == key_value]
        if not target_row.empty:
            target_row = target_row.iloc[0]
            target_pincode = target_row['pincode'] if 'pincode' in target_row and pd.notnull(target_row['pincode']) else "Pincode missing"
            target_state = target_row['state'] if 'state' in target_row and pd.notnull(target_row['state']) else "State missing"
            target_city = target_row['city'] if 'city' in target_row and pd.notnull(target_row['city']) else "City missing"
            target_details = f"Pincode: {target_pincode}, State: {target_state}, City: {target_city}"
        else:
            target_details = f"Key '{key_column}' with value '{key_value}' not present in target table '{target_table}'."

        # Check if pincode exists in reference
        ref_matches = reference_df[reference_df['pincode'] == pincode]
        if ref_matches.empty:
            issue = f"Invalid pincode ({pincode})."
            pincode_mapping_issues = pd.concat([pincode_mapping_issues, pd.DataFrame([{
                key_column: key_value,
                'pincode': pincode,
                'state': state,
                'city': city,
                'issue': issue,
                f'{target_table}_details': target_details
            }])], ignore_index=True)
            error_detail = {
                'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
                'issue': 'pincode_mapping',
                'error_message': f"{issue}. {target_table} Details: {target_details}",
                'source_table': master_table,
                'target_table': target_table,
                'issue_column': 'pincode',
                'unique_identifier': f'{key_column}: {key_value}'
            }
            error_logs_m.append(error_detail)
            continue

        # Check if any of the reference entries match both the city and state
        exact_match = ref_matches[
            (ref_matches['city'] == city) & (ref_matches['state'] == state)
        ]
        if not exact_match.empty:
            continue  # No issue, mapping is correct

        # Check for state mismatch
        state_matches = ref_matches[ref_matches['state'] == state]

        # Check for city mismatch
        city_matches = ref_matches[ref_matches['city'] == city]

        if state_matches.empty and city_matches.empty:
            # Both state and city do not match
            expected_entries = ref_matches[['state', 'city']].drop_duplicates()
            expected_states = expected_entries['state'].tolist()
            expected_cities = expected_entries['city'].tolist()
            expected_states_str = ', '.join(expected_states)
            expected_cities_str = ', '.join(expected_cities)
            issue = f"Pincode {pincode} does not match state '{state}' and city '{city}'. Expected states: {expected_states_str}; Expected cities: {expected_cities_str}."
        elif state_matches.empty:
            # State does not match
            expected_states = ref_matches['state'].unique().tolist()
            expected_states_str = ', '.join(expected_states)
            issue = f"Pincode {pincode} does not match state '{state}'. Expected states: {expected_states_str}."
        elif city_matches.empty:
            # City does not match
            expected_cities = state_matches['city'].unique().tolist()
            expected_cities_str = ', '.join(expected_cities)
            issue = f"Pincode {pincode} does not match city '{city}'. Expected cities: {expected_cities_str}."
        else:
            # Other cases
            issue = f"Pincode {pincode} has a mapping inconsistency."

        pincode_mapping_issues = pd.concat([pincode_mapping_issues, pd.DataFrame([{
            key_column: key_value,
            'pincode': pincode,
            'state': state,
            'city': city,
            'issue': issue,
            f'{target_table}_details': target_details
        }])], ignore_index=True)
        error_detail = {
            'time_stamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'issue': 'pincode_mapping',
            'error_message': f"{issue}. {target_table} Details: {target_details}",
            'source_table': master_table,
            'target_table': target_table,
            'issue_column': 'pincode',
            'unique_identifier': f'{key_column}: {key_value}'
        }
        error_logs_m.append(error_detail)
    
    logging.info(f"Found {len(pincode_mapping_issues)} pincode mapping issues in master table '{master_table}'.")
    return pincode_mapping_issues, error_logs_m

def compare_tables(client, dataset_name, base_name, master_table, target_table, master_key, target_key):
    """
    Compare two tables and generate a report.

    Args:
        client (bigquery.Client): Initialized BigQuery client.
        dataset_name (str): The name of the dataset.
        base_name (str): The base name of the table.
        master_table (str): Name of the master_hub_ table.
        target_table (str): Name of the target prefixed table.
        master_key (str): The key column in the master table.
        target_key (str): The key column in the target table.

    Returns:
        dict: A dictionary containing all comparison results.
    """
    logging.info(f"Starting comparison for base table '{base_name}': '{master_table}' vs '{target_table}'.")

    # Initialize comparison results
    mismatches = []
    null_values_master = []
    null_values_target = []
    data_type_issues = pd.DataFrame()
    format_issues_master = pd.DataFrame()
    pincode_mapping_issues = pd.DataFrame()
    duplicates_master = pd.DataFrame()
    duplicates_target = pd.DataFrame()
    master_only_keys = []
    target_only_keys = []

    # Load data
    df_master = load_table_from_bigquery(client, dataset_name, master_table)
    df_target = load_table_from_bigquery(client, dataset_name, target_table)


    # **Apply configurable active filter if defined**
    base_table_info = BASE_TABLES.get(base_name, {})
    active_filter = base_table_info.get('active_filter')
    # Determine whether to perform additional checks
    perform_checks = base_table_info.get('perform_checks', True)

    if active_filter:
        column = active_filter.get('column')
        value = active_filter.get('value')
        if column and column in df_master.columns:
            initial_count = len(df_master)
            df_master = df_master[df_master[column] == value]
            filtered_count = len(df_master)
            logging.info(f"Filtered '{base_name}' master table: {initial_count - filtered_count} records excluded based on {column} = {value}.")
        else:
            logging.warning(f"Active filter specified but column '{column}' not found in master table '{master_table}'.")

    if df_master.empty or df_target.empty:
        logging.warning(f"One of the tables '{master_table}' or '{target_table}' is empty. Skipping comparison.")
        return None

    # Identify BigNumeric columns in master and target tables
    schema_master = get_table_schema(client, dataset_name, master_table)
    schema_target = get_table_schema(client, dataset_name, target_table)
    bignumeric_columns_master = [col for col, dtype in schema_master.items() if dtype == 'BIGNUMERIC']
    bignumeric_columns_target = [col for col, dtype in schema_target.items() if dtype == 'BIGNUMERIC']

    # Format BigNumeric columns in master table
    for col in bignumeric_columns_master:
        if col in df_master.columns:
            df_master[col] = df_master[col].apply(lambda x: format(x, '.0f') if pd.notnull(x) else x)

    # Format BigNumeric columns in target table
    for col in bignumeric_columns_target:
        if col in df_target.columns:
            df_target[col] = df_target[col].apply(lambda x: format(x, '.0f') if pd.notnull(x) else x)

    # Identify common columns
    common_columns, master_unique_cols, target_unique_cols = find_common_and_non_common_columns(df_master, df_target)

    # Get imp_columns and non_imp_columns
    imp_columns = Imp_columns.get(base_name, None)
    non_imp_columns = Non_imp_columns.get(base_name, [])

    # Determine columns to check based on imp_columns
    if imp_columns:
        columns_to_check = [col for col in imp_columns if col in common_columns]
        logging.info(f"Important columns defined for '{base_name}': {columns_to_check}")
    else:
        columns_to_check = [col for col in common_columns if col not in non_imp_columns]
        logging.info(f"No important columns defined for '{base_name}'. Applying checks to all columns except non_imp_columns: {columns_to_check}")

    if perform_checks:
        # Find duplicates in both tables
        duplicates_master, error_logs_m = find_duplicates(df_master, master_key, master_table)
        ERROR_LOG_M.extend(error_logs_m)
        duplicates_target,  error_logs_m = find_duplicates(df_target, target_key, target_table)
        ERROR_LOG_M.extend(error_logs_m)

    if not duplicates_master.empty:
        logging.warning(f"Duplicate keys found in source table '{master_table}'. These will be reported but not used in mismatch comparison.")
    if not duplicates_target.empty:
        logging.warning(f"Duplicate keys found in target table '{target_table}'. These will be reported but not used in mismatch comparison.")

    if not common_columns:
        logging.warning(f"No common columns found between '{master_table}' and '{target_table}'. Skipping comparison.")
        return None
    
    # Retrieve non-important columns for the current base table
    non_imp_columns = Non_imp_columns.get(base_name, [])

    # Perform mismatch comparison if allowed
    if perform_checks:
        # Find mismatches excluding duplicate keys
        mismatches, error_logs_m = find_mismatches(
            df_master,
            df_target,
            columns_to_check,
            master_key,
            target_key,
            master_table,
            target_table,
            duplicates_master,
            duplicates_target,
            non_imp_columns
        )
        ERROR_LOG_M.extend(error_logs_m)
    # Find detailed null values in both tables if allowed
    if perform_checks:    
        # Find detailed null values in both tables
        null_values_master, null_values_target, error_logs_m = find_detailed_nulls(
            df_master,
            df_target,
            master_key,
            target_key,
            master_table,
            target_table,
            columns_to_check
        )
        ERROR_LOG_M.extend(error_logs_m)

    if perform_checks:    
        # Validate data types between master and target schemas
        data_type_issues, error_logs_m = validate_data_types(schema_master, schema_target, master_key, master_table, target_table, columns_to_check)
        ERROR_LOG_M.extend(error_logs_m)

    if perform_checks:        
        # Validate formats in master table only and include target values
        format_issues_master, error_logs_m = validate_formats(df_master, df_target, master_key, target_key, target_table, master_table)
        ERROR_LOG_M.extend(error_logs_m)
        
    if perform_checks:        
        # Validate pincode mapping if applicable and include target values
        pincode_mapping_issues = pd.DataFrame()
        if {'pincode', 'city', 'state'}.issubset(df_master.columns):
            pincode_mapping_issues, error_logs_m = validate_pincode_mapping(
                df_master, 
                df_target, 
                master_key, 
                target_key, 
                target_table, 
                client, master_table
            )
            ERROR_LOG_M.extend(error_logs_m)

    # Find non-matching keys
    master_only_keys, target_only_keys, error_logs_m = find_non_matching_keys(
        df_master, df_target, master_key, target_key, duplicates_master, duplicates_target, master_table, target_table
    )
    ERROR_LOG_M.extend(error_logs_m)

    # Compile results
    results = {
        'mismatches': pd.DataFrame(mismatches),
        'null_values_master': pd.DataFrame(null_values_master),
        'null_values_target': pd.DataFrame(null_values_target),
        'duplicates_master': duplicates_master,
        'duplicates_target': duplicates_target,
        'data_type_issues': data_type_issues,
        'format_issues_master': format_issues_master,
        'pincode_mapping_issues': pincode_mapping_issues,
        'key_column_master': master_key,
        'key_column_target': target_key,
        'df_master_only_keys': master_only_keys,
        'df_target_only_keys': target_only_keys,
        'table1_name': master_table,
        'table2_name': target_table,
        'df_master': df_master,
        'df_target': df_target
    }

    logging.info(f"Completed comparison for '{master_table}' vs '{target_table}'.")
    return results

def generate_string_schema(df):
    """
    Generates a BigQuery schema with all fields as STRING.
    
    Args:
        df (pd.DataFrame): The DataFrame for which to generate the schema.
        
    Returns:
        list: A list of SchemaField objects with type STRING.
    """
    schema = [SchemaField(column, "STRING", mode="NULLABLE") for column in df.columns]
    return schema

def _upload_dataframe_to_bigquery(client, analytics_dataset, table_name, df):
    """
    Helper function to upload a DataFrame to BigQuery.
    
    Args:
        client (bigquery.Client): Initialized BigQuery client.
        analytics_dataset (str): The name of the Analytics dataset.
        table_name (str): The name of the table to upload.
        df (pd.DataFrame): The DataFrame to upload.
        
    Returns:
        None
    """
    if df.empty:
        logging.info(f"No data to upload for '{table_name}'. Skipping.")
        return
    
    # Convert all columns to string type
    df = df.astype(str)
    
    # Generate BigQuery schema with all fields as STRING
    schema = generate_string_schema(df)
    
    # Ensure table name doesn't exceed BigQuery's maximum length (1,024 characters)
    if len(table_name) > 1024:
        original_table_name = table_name
        table_name = table_name[:1021] + '...'
        logging.warning(f"Table name truncated from '{original_table_name}' to '{table_name}' due to length constraints.")
    
    # Define the full table ID
    table_id = f"{client.project}.{analytics_dataset}.{table_name}"
    
    # Upload the DataFrame to BigQuery
    try:
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema  # Using the provided schema with all fields as STRING
            )
        )
        job.result()  # Wait for the job to complete
        logging.info(f"Successfully uploaded '{table_id}' with {len(df)} records.")
    except Exception as e:
        logging.error(f"Failed to upload '{table_id}' to BigQuery: {e}")

def upload_comparison_results_to_bigquery(client, analytics_dataset, ERROR_LOG_M):
    """
    Uploads each part of the comparison_result to BigQuery as separate tables in the Analytics dataset.
    The table names follow the format: 'table1_name_vs_table2_name_heading'.
    
    Args:
        client (bigquery.Client): Initialized BigQuery client.
        analytics_dataset (str): The name of the Analytics dataset.
        ERROR_LOG_M (list): The error log data as a list of dictionaries.
        
    Returns:
        None
    """
    
    # Handle ERROR_LOG separately
    if ERROR_LOG_M:
        # Convert to DataFrame
        error_df = pd.DataFrame(ERROR_LOG_M)
        _upload_dataframe_to_bigquery(client, analytics_dataset, "error_logs_supplier", error_df)
    else:
        logging.info("No error logs to upload.")

def main():
    """
    Main function to orchestrate the comparison of multiple base tables against their master_hub_ counterparts.
    """
    try:
        # Initialize BigQuery client
        try:
            client = get_bigquery_client(PROJECT_ID)
        except Exception:
            logging.error("Exiting due to BigQuery client initialization failure.")
            return

        # Find common tables with 'master_hub_' and other prefixes, passing BASE_TABLES
        common_tables = find_common_tables_with_master_hub(client, DATASET_ID, PREFIXES, BASE_TABLES)
        
        if not common_tables:
            logging.info("No common tables found with 'master_hub_' and the specified prefixes.")
            return

        # Iterate over each base table and perform comparisons
        for base_name, tables in common_tables.items():
            base_table_info = BASE_TABLES.get(base_name)
            if not base_table_info:
                logging.warning(f"No configuration found for base table '{base_name}'. Skipping.")
                continue

            master_key = base_table_info.get('master_key')
            target_tables = base_table_info.get('targets', {})
            
            master_table = tables.get('master_hub_')
            if not master_table:
                logging.warning(f"Master table 'master_hub_{base_name}' not found. Skipping.")
                continue

            # Iterate through each prefix and its corresponding target_key
            for prefix, target_key in target_tables.items():
                target_table = tables.get(prefix)
                if not target_table:
                    logging.warning(f"Target table with prefix '{prefix}' for base table '{base_name}' not found. Skipping.")
                    continue
                
                comparison_result = compare_tables(
                    client, 
                    DATASET_ID, 
                    base_name, 
                    master_table, 
                    target_table, 
                    master_key, 
                    target_key  # Pass the correct target_key per prefix
                )
                if comparison_result:
                    # Generate individual Excel report for this comparison
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    base_name_formatted = base_name.replace(' ', '_').lower()
                    report_filename = f"Master_Data_validation_{timestamp}.xlsx"
                    report_filepath = os.path.join(os.getcwd(), report_filename)
                    
                    create_excel_report(comparison_result, report_filepath)
                    
                    # Upload the report to Slack
                    upload_file_to_slack(report_filepath, title=f"{base_name.capitalize()} Comparison Report: {master_table} vs {target_table}")
                    
                    # Remove the local report file after successful upload
                    try:
                        os.remove(report_filepath)
                        logging.info(f"Removed local report file '{report_filepath}'.")
                    except Exception as e:
                        logging.error(f"Failed to remove local report file '{report_filepath}': {e}")
                    
                    # Send a Slack message summarizing the comparison
                    total_mismatches = len(comparison_result['mismatches'])
                    total_nulls_master = len(comparison_result['null_values_master'])
                    total_nulls_target = len(comparison_result['null_values_target'])
                    total_dup_master = len(comparison_result['duplicates_master'])
                    total_dup_target = len(comparison_result['duplicates_target'])
                    total_data_type_issues = len(comparison_result['data_type_issues'])
                    total_format_issues_master = len(comparison_result['format_issues_master'])
                    total_pincode_issues = len(comparison_result['pincode_mapping_issues'])
                    total_non_matching_source = len(comparison_result.get('df_master_only_keys', []))
                    total_non_matching_target = len(comparison_result.get('df_target_only_keys', []))
                    
                    message = (
                        f"✅ *Comparison Report Generated for `{base_name}`*\n"
                        f"*Tables Compared: `{comparison_result['table1_name']}` vs `{comparison_result['table2_name']}`*\n"
                        f"- *Total Pincode Mapping Issues in `{comparison_result['table1_name']}`: `{total_pincode_issues}`*\n"
                        "- *Non-Matching Keys*:\n"
                        f"  -- *`{master_key}` only in `{comparison_result['table1_name']}` and not in `{comparison_result['table2_name']}`: `{total_non_matching_source}`*\n"
                        f"  -- *`{target_key}` only in `{comparison_result['table2_name']}` and not in `{comparison_result['table1_name']}`: `{total_non_matching_target}`*\n"
                        f"- *Total Mismatches between values of same column name of both tables: `{total_mismatches}`*\n"
                        f"- *Total Null Values in `{comparison_result['table1_name']}`: `{total_nulls_master}`*\n"
                        f"- *Total Null Values in `{comparison_result['table2_name']}`: `{total_nulls_target}`*\n"
                        f"- *Duplicate `{master_key}` in `{comparison_result['table1_name']}`: `{total_dup_master}`*\n"
                        f"- *Duplicate `{target_key}` in `{comparison_result['table2_name']}`: `{total_dup_target}`*\n"
                        f"- *Total Data Type Issues (mismatch between datatypes in columns with same name of both tables): `{total_data_type_issues}`*\n"
                        f"- *Total Format/Value Issues (GSTIN, email, pincode) in `{comparison_result['table1_name']}`: `{total_format_issues_master}`*\n"
                    )
                
                    send_slack_alert(message)
                    
                    # Optional: Pause between uploads to avoid rate limits
                    time.sleep(30)  # Adjust sleep time as needed
                else:
                    logging.info(f"No comparison results to report for '{master_table}' vs '{target_table}'.")

        # Upload error logs to BigQuery after all comparisons
        upload_comparison_results_to_bigquery(
            client, 
            # 'Analytics',
            'analytics_data',
            ERROR_LOG_M
        )
        

        logging.info("All comparisons completed.")
    except Exception as e:
        # Capture the full traceback
        tb = traceback.format_exc()
        logging.error("An unexpected error occurred in the main process.", exc_info=True)
        
        # Prepare a detailed error message for Slack
        error_message = (
            f"❌ *Comparison Process Failed*\n"
            f"*Error:* {str(e)}\n"
            f"*Traceback:*\n```{tb}```"
        )
        send_slack_alert(error_message)

        # Optionally, exit the script with a non-zero status
        sys.exit(1)
        
if __name__ == "__main__":
    main()
