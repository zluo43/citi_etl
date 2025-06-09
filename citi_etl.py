import os
import pandas as pd # For sample_df.dtype checks, DuckDB's fetchdf() returns a pandas DF
# import geopandas as gpd # Not directly used by this version of the pipeline functions
import wget
import zipfile
from datetime import datetime, timedelta # Not directly used by this version of the pipeline functions
import re
# import leafmap # Commented out as in original user code
# import lonboard # Commented out as in original user code
# import h3 # Commented out as in original user code
# import requests # Not directly used by this version of the pipeline functions
import duckdb
import shutil

# Function to generate file names based on year and month ranges
def generate_file_names(start_year_param, end_year_param, end_month_for_final_year_param):
    """
    Generates a list of Citi Bike data file URLs based on specified year and month ranges
    and known naming conventions.

    Args:
        start_year_param (int): The first year to generate filenames for.
        end_year_param (int): The last year to generate filenames for.
        end_month_for_final_year_param (int): The last month to generate filenames for,
                                               only applicable to the end_year_param.

    Returns:
        list: A list of URLs for the Citi Bike data files.
    """
    base_url = "https://s3.amazonaws.com/tripdata/"
    local_file_list = []

    for year_iter in range(start_year_param, end_year_param + 1):
        num_months_to_iterate = 12
        if year_iter == end_year_param:
            num_months_to_iterate = end_month_for_final_year_param

        if year_iter < 2024:
            file_name = f"{base_url}{year_iter}-citibike-tripdata.zip"
            local_file_list.append(file_name)
        elif year_iter == 2024:
            for month_iter in range(1, num_months_to_iterate + 1):
                if month_iter in [5, 6, 7, 8, 9, 10]:
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                else:
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                local_file_list.append(file_name)
        elif year_iter == 2025:
            for month_iter in range(1, num_months_to_iterate + 1):
                if month_iter == 3:  # March 2025
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                else:
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                local_file_list.append(file_name)
        # Add rules for other years if needed
    return local_file_list

# Generator function to download, extract files, and yield CSV paths
def download_and_extract_files_generator(url_list, destination_folder):
    """
    Downloads, unzips Citi Bike data files, including one level of nested zips,
    and yields the paths to extracted CSV files.

    Args:
        url_list (list): A list of URLs to download.
        destination_folder (str): The folder to download and extract files into.

    Yields:
        str: The full path to an extracted CSV file.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        print(f"Created destination folder: {destination_folder}")

    for url in url_list:
        downloaded_zip_path = None
        # This list will collect CSV paths from the current top-level zip and its direct nested zips
        csv_files_from_current_top_zip = []
        try:
            print(f"Attempting to download: {url}")
            downloaded_zip_path = wget.download(url, out=destination_folder)
            print(f"\nFile downloaded: {downloaded_zip_path}")

            with zipfile.ZipFile(downloaded_zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.startswith('__MACOSX/') or member.endswith('.DS_Store') or member.endswith('/'): # Skip directories
                        continue
                    
                    # Extract the member from the top-level zip
                    zip_ref.extract(member, destination_folder)
                    extracted_member_path = os.path.join(destination_folder, member)

                    if not os.path.exists(extracted_member_path): # Should not happen if extract was successful
                        print(f"Warning: Extracted member path does not exist: {extracted_member_path}")
                        continue

                    # Check if the extracted member is a CSV file
                    if extracted_member_path.lower().endswith('.csv') and os.path.isfile(extracted_member_path):
                        csv_files_from_current_top_zip.append(extracted_member_path)
                        print(f"Successfully extracted CSV: {extracted_member_path}")
                    
                    # Check if the extracted member is a ZIP file (nested zip)
                    elif extracted_member_path.lower().endswith('.zip') and os.path.isfile(extracted_member_path):
                        print(f"Found nested zip: {extracted_member_path}. Extracting its contents...")
                        nested_zip_temp_path = extracted_member_path # Path to the extracted nested zip
                        try:
                            with zipfile.ZipFile(nested_zip_temp_path, 'r') as nested_zip_ref:
                                for nested_member_name in nested_zip_ref.namelist():
                                    if nested_member_name.startswith('__MACOSX/') or nested_member_name.endswith('.DS_Store') or nested_member_name.endswith('/'):
                                        continue
                                    
                                    # Extract member from the nested zip
                                    nested_zip_ref.extract(nested_member_name, destination_folder)
                                    extracted_nested_file_path = os.path.join(destination_folder, nested_member_name)

                                    if not os.path.exists(extracted_nested_file_path):
                                        print(f"Warning: Extracted nested member path does not exist: {extracted_nested_file_path}")
                                        continue

                                    if extracted_nested_file_path.lower().endswith('.csv') and os.path.isfile(extracted_nested_file_path):
                                        csv_files_from_current_top_zip.append(extracted_nested_file_path)
                                        print(f"Successfully extracted nested CSV: {extracted_nested_file_path}")
                                    elif os.path.isfile(extracted_nested_file_path): # It's a file, but not a CSV
                                        print(f"Extracted non-CSV file from nested zip: {extracted_nested_file_path}")
                                        # Optionally, delete non-CSV files from nested zips if they are not needed
                                        # os.remove(extracted_nested_file_path)
                        except zipfile.BadZipFile:
                            print(f"Error: Nested file {nested_zip_temp_path} is not a valid zip file or is corrupted.")
                        except Exception as e_nested_unzip:
                            print(f"Error processing nested zip {nested_zip_temp_path}: {e_nested_unzip}")
                        finally:
                            # Delete the intermediate nested zip file after attempting to process it
                            if os.path.exists(nested_zip_temp_path):
                                try:
                                    os.remove(nested_zip_temp_path)
                                    print(f"Nested zip file deleted: {nested_zip_temp_path}")
                                except Exception as e_del_nested:
                                    print(f"Error deleting nested zip file {nested_zip_temp_path}: {e_del_nested}")
                    elif os.path.isfile(extracted_member_path): # It's a file, but not a CSV or ZIP
                         print(f"Extracted non-CSV/non-ZIP file: {extracted_member_path}")
                         # Optionally, delete these files if not needed
                         # os.remove(extracted_member_path)


            if not csv_files_from_current_top_zip:
                print(f"Warning: No CSV files found in or under {downloaded_zip_path}")

            # Yield all collected CSV paths from this top-level zip processing
            for csv_file_path in csv_files_from_current_top_zip:
                yield csv_file_path

        except zipfile.BadZipFile:
             print(f"Error: Downloaded file {url} is not a valid zip file or is corrupted.")
        except Exception as e:
            print(f"Error processing URL: {url}")
            print(f"Error message: {str(e)}")
        finally:
            # Delete the main downloaded zip file
            if downloaded_zip_path and os.path.exists(downloaded_zip_path):
                try:
                    os.remove(downloaded_zip_path)
                    print(f"Top-level zip file deleted: {downloaded_zip_path}")
                except Exception as e:
                    print(f"Error deleting top-level zip file {downloaded_zip_path}: {str(e)}")
            print("-" * 30) # Separator for each URL processed

    # Clean up __MACOSX folder if it exists at the end
    macosx_folder = os.path.join(destination_folder, '__MACOSX')
    if os.path.exists(macosx_folder) and os.path.isdir(macosx_folder):
        try:
            shutil.rmtree(macosx_folder)
            print(f"Removed folder: {macosx_folder}")
        except Exception as e:
            print(f"Error removing __MACOSX folder: {str(e)}")


# Function to process a single CSV and load it into DuckDB
def process_csv_to_duckdb(csv_file_path, db_connection):
    """
    Processes a single CSV file, standardizes its schema, and loads it into DuckDB.
    This full version includes all column detection and appends data to existing tables.
    """
    filename = os.path.basename(csv_file_path)
    print(f"Processing CSV: {filename}")

    # Regex to extract YYYYMM from the filename.
    date_match = re.search(r'(20\d{2})(\d{2})', filename)
    
    table_name_suffix = ""
    if date_match:
        year_str, month_str = date_match.groups()
        table_name_suffix = f"{year_str}_{month_str}"
    else:
        # Fallback for any unexpected filename formats
        generic_name = re.sub(r'[^a-zA-Z0-9_]', '_', os.path.splitext(filename)[0])
        table_name_suffix = f"unknown_date_{generic_name}"
        print(f"Could not extract YYYYMM from filename: {filename}. Using suffix: {table_name_suffix}")

    try:
        sample_query = f"SELECT * FROM read_csv_auto('{csv_file_path}', header=true, sample_size=100, ignore_errors=true)"
        sample_df = db_connection.execute(sample_query).fetchdf()
    except Exception as e:
        print(f"Error reading sample from {filename}: {str(e)}. Skipping file.")
        return

    if sample_df.empty:
        print(f"Sample from {filename} is empty or could not be read. Skipping file.")
        return

    select_statement = None
    final_table_name = ""
    
    # --- Schema Detection and SELECT Statement Construction ---
    if 'member_casual' in sample_df.columns:
        final_table_name = f"citibike_data_{table_name_suffix}_new_schema"
        select_statement = f"""
        SELECT
            "ride_id"::VARCHAR AS ride_id,
            "rideable_type"::VARCHAR AS rideable_type,
            "started_at"::TIMESTAMP AS started_at,
            "ended_at"::TIMESTAMP AS ended_at,
            "start_station_name"::VARCHAR AS start_station_name,
            "start_station_id"::VARCHAR AS start_station_id,
            "end_station_name"::VARCHAR AS end_station_name,
            "end_station_id"::VARCHAR AS end_station_id,
            "start_lat"::DOUBLE AS start_lat,
            "start_lng"::DOUBLE AS start_lng,
            "end_lat"::DOUBLE AS end_lat,
            "end_lng"::DOUBLE AS end_lng,
            "member_casual"::VARCHAR AS member_casual
        FROM read_csv('{csv_file_path}', header=true, ignore_errors=true, types={{'start_station_id': 'VARCHAR', 'end_station_id': 'VARCHAR'}})
        """
    elif 'gender' in sample_df.columns or 'Gender' in sample_df.columns:
        final_table_name = f"citibike_data_{table_name_suffix}_old_schema"
        
        def get_col_name(potential_names, df_cols_list):
            col_map = {col.lower(): col for col in df_cols_list}
            col_map.update({col.lower().replace(' ', ''): col for col in df_cols_list})

            for name_variant in potential_names:
                if name_variant in df_cols_list: return name_variant
                if name_variant.lower() in col_map: return col_map[name_variant.lower()]
                if name_variant.lower().replace(' ', '') in col_map: return col_map[name_variant.lower().replace(' ', '')]
            return None

        s_cols = sample_df.columns.tolist()
        start_time_col = get_col_name(['starttime', 'Start Time'], s_cols)
        stop_time_col = get_col_name(['stoptime', 'Stop Time'], s_cols)
        start_station_id_col = get_col_name(['start station id', 'Start Station ID'], s_cols)
        start_station_name_col = get_col_name(['start station name', 'Start Station Name'], s_cols)
        start_station_lat_col = get_col_name(['start station latitude', 'Start Station Latitude'], s_cols)
        start_station_lng_col = get_col_name(['start station longitude', 'Start Station Longitude'], s_cols)
        end_station_id_col = get_col_name(['end station id', 'End Station ID'], s_cols)
        end_station_name_col = get_col_name(['end station name', 'End Station Name'], s_cols)
        end_station_lat_col = get_col_name(['end station latitude', 'End Station Latitude'], s_cols)
        end_station_lng_col = get_col_name(['end station longitude', 'End Station Longitude'], s_cols)
        bikeid_col = get_col_name(['bikeid', 'Bike ID'], s_cols)
        usertype_col = get_col_name(['usertype', 'User Type'], s_cols)
        birth_year_col = get_col_name(['birth year', 'Birth Year'], s_cols)
        gender_col = get_col_name(['gender', 'Gender'], s_cols)

        required_cols_present = all([
            start_time_col, stop_time_col, start_station_id_col, start_station_name_col,
            start_station_lat_col, start_station_lng_col, end_station_id_col, end_station_name_col,
            end_station_lat_col, end_station_lng_col, bikeid_col, usertype_col, gender_col
        ])

        if not required_cols_present:
            print(f"Missing one or more critical columns for old schema in {filename}. Skipping.")
            return

        is_starttime_string = start_time_col and start_time_col in sample_df and sample_df[start_time_col].dtype == 'object'
        
        current_read_csv_types = {
            f'"{start_station_id_col}"': 'VARCHAR', f'"{end_station_id_col}"': 'VARCHAR',
            f'"{end_station_lat_col}"': 'VARCHAR', f'"{end_station_lng_col}"': 'VARCHAR'
        }

        if is_starttime_string:
            if start_time_col: current_read_csv_types[f'"{start_time_col}"'] = 'VARCHAR'
            if stop_time_col: current_read_csv_types[f'"{stop_time_col}"'] = 'VARCHAR'
            time_select_sql = f"""
                CASE
                    WHEN "{start_time_col}" ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}} \\d{{1,2}}:\\d{{1,2}}:\\d{{1,2}}$' THEN strptime("{start_time_col}", '%m/%d/%Y %H:%M:%S')
                    WHEN "{start_time_col}" ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}} \\d{{1,2}}:\\d{{1,2}}$' THEN strptime("{start_time_col}", '%m/%d/%Y %H:%M')
                    WHEN "{start_time_col}" ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}(\\.\\d{{1,6}})?$' THEN strptime("{start_time_col}", '%Y-%m-%d %H:%M:%S.%f')
                    WHEN "{start_time_col}" ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}$' THEN strptime("{start_time_col}", '%Y-%m-%d %H:%M:%S')
                    ELSE NULL
                END AS starttime,
                CASE
                    WHEN "{stop_time_col}" ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}} \\d{{1,2}}:\\d{{1,2}}:\\d{{1,2}}$' THEN strptime("{stop_time_col}", '%m/%d/%Y %H:%M:%S')
                    WHEN "{stop_time_col}" ~ '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}} \\d{{1,2}}:\\d{{1,2}}$' THEN strptime("{stop_time_col}", '%m/%d/%Y %H:%M')
                    WHEN "{stop_time_col}" ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}(\\.\\d{{1,6}})?$' THEN strptime("{stop_time_col}", '%Y-%m-%d %H:%M:%S.%f')
                    WHEN "{stop_time_col}" ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}$' THEN strptime("{stop_time_col}", '%Y-%m-%d %H:%M:%S')
                    ELSE NULL
                END AS stoptime
            """
        else: 
            time_select_sql = f"""
                "{start_time_col}"::TIMESTAMP AS starttime,
                "{stop_time_col}"::TIMESTAMP AS stoptime
            """
        
        types_str_parts = [f"'{k.strip().stripchr('\"')}': '{v}'" for k, v in current_read_csv_types.items() if k.strip().stripchr('"') in s_cols]
        types_dict_str = f"{{{', '.join(types_str_parts)}}}"
        read_csv_options_old_schema = f"'{csv_file_path}', header=true, ignore_errors=true, types={types_dict_str}, auto_detect=true"

        select_statement = f"""
        SELECT
            {time_select_sql},
            "{start_station_id_col}"::VARCHAR AS start_station_id,
            "{start_station_name_col}"::VARCHAR AS start_station_name,
            "{start_station_lat_col}"::DOUBLE AS start_station_latitude,
            "{start_station_lng_col}"::DOUBLE AS start_station_longitude,
            "{end_station_id_col}"::VARCHAR AS end_station_id,
            "{end_station_name_col}"::VARCHAR AS end_station_name,
            NULLIF(CAST("{end_station_lat_col}" AS VARCHAR), 'NULL')::DOUBLE AS end_station_latitude,
            NULLIF(CAST("{end_station_lng_col}" AS VARCHAR), 'NULL')::DOUBLE AS end_station_longitude,
            "{bikeid_col}"::BIGINT AS bikeid,
            "{usertype_col}"::VARCHAR AS usertype,
            CASE 
                WHEN "{birth_year_col}" IS NOT NULL AND TRIM(CAST("{birth_year_col}" AS VARCHAR)) != '' 
                THEN LEFT(TRIM(CAST("{birth_year_col}" AS VARCHAR)), 4) 
                ELSE NULL 
            END::VARCHAR AS birth_year,
            "{gender_col}"::BIGINT AS gender
        FROM read_csv({read_csv_options_old_schema})
        """
    else:
        print(f"Unknown schema for file: {filename} (sample columns: {sample_df.columns.tolist()}). Skipping.")
        return

    # --- Conditional INSERT INTO or CREATE TABLE logic ---
    if select_statement and final_table_name:
        try:
            table_exists_query = f"SELECT 1 FROM duckdb_tables() WHERE table_name = '{final_table_name}' LIMIT 1"
            table_exists = db_connection.execute(table_exists_query).fetchone()

            if table_exists:
                full_query = f'INSERT INTO "{final_table_name}" ({select_statement})'
                print(f"Appending data to existing table: {final_table_name}")
            else:
                full_query = f'CREATE TABLE "{final_table_name}" AS ({select_statement})'
                print(f"Creating new table: {final_table_name}")
            
            db_connection.execute(full_query)
            print(f"Successfully processed and stored data from {filename} into {final_table_name}")
        except Exception as e:
            print(f"Error executing query for {final_table_name} from {filename}: {str(e)}")
    else:
        print(f"No insert query generated for {filename}.")


# Function to combine tables, add geometry, and export to Parquet
def convert_parquet(db_connection, output_parquet_base_dir):
    """
    Combines tables in DuckDB by schema type, adds geometry, and exports to partitioned Parquet.
    """
    if not os.path.exists(output_parquet_base_dir):
        os.makedirs(output_parquet_base_dir)
        print(f"Created Parquet output directory: {output_parquet_base_dir}")

    try:
        db_connection.install_extension("spatial")
        db_connection.load_extension("spatial")
    except Exception as e:
        print(f"Error loading spatial extension: {e}. Geospatial operations might fail.")

    tables_df = db_connection.execute("SHOW TABLES").fetchdf()
    if tables_df.empty:
        print("No tables found in the database to convert to Parquet.")
        return

    actual_tables = [name for name in tables_df['name'] if db_connection.execute(f"SELECT type FROM duckdb_tables() WHERE table_name = '{name}'").fetchone()[0] == 'BASE TABLE']

    old_schema_tables = [name for name in actual_tables if '_old_schema' in name]
    new_schema_tables = [name for name in actual_tables if '_new_schema' in name]

    schema_map = {
        "old_schema_combined": {
            "tables": old_schema_tables,
            "start_lng_col": "start_station_longitude", "start_lat_col": "start_station_latitude",
            "end_lng_col": "end_station_longitude", "end_lat_col": "end_station_latitude",
            "time_col": "starttime"
        },
        "new_schema_combined": {
            "tables": new_schema_tables,
            "start_lng_col": "start_lng", "start_lat_col": "start_lat",
            "end_lng_col": "end_lng", "end_lat_col": "end_lat",
            "time_col": "started_at"
        }
    }

    for combined_name, details in schema_map.items():
        if not details["tables"]:
            print(f"No tables found for schema {combined_name}. Skipping Parquet conversion for this schema.")
            continue

        print(f"Combining tables for {combined_name}...")
        union_parts = [f'SELECT * FROM "{table_name}"' for table_name in details["tables"]]
        
        if not union_parts:
            print(f"No tables to union for {combined_name}.")
            continue

        combine_query = f'CREATE OR REPLACE TABLE "{combined_name}" AS { " UNION ALL ".join(union_parts) }'
        try:
            db_connection.execute(combine_query)
            print(f"Created combined table: {combined_name}")
        except Exception as e:
            print(f"Error combining tables for {combined_name}: {str(e)}\nQuery: {combine_query}")
            continue

        table_with_geom_name = f"{combined_name}_with_geom"
        # Check if the combined table is empty before proceeding
        count_check = db_connection.execute(f'SELECT COUNT(*) FROM "{combined_name}"').fetchone()
        if count_check and count_check[0] == 0:
            print(f"Combined table {combined_name} is empty. Skipping geometry addition and Parquet export.")
            continue

        add_geom_query = f"""
        CREATE OR REPLACE TABLE "{table_with_geom_name}" AS
        SELECT *,
               st_aswkb(st_point("{details["end_lng_col"]}", "{details["end_lat_col"]}"))::BLOB AS end_geom,
               st_aswkb(st_point("{details["start_lng_col"]}", "{details["start_lat_col"]}"))::BLOB AS start_geom,
               YEAR("{details["time_col"]}") AS year,
               MONTH("{details["time_col"]}") AS month
        FROM "{combined_name}"
        WHERE "{details["time_col"]}" IS NOT NULL
          AND "{details["start_lng_col"]}" IS NOT NULL AND "{details["start_lat_col"]}" IS NOT NULL
          AND "{details["end_lng_col"]}" IS NOT NULL AND "{details["end_lat_col"]}" IS NOT NULL
          AND typeof("{details["start_lng_col"]}") NOT IN ('VARCHAR', 'NULL') 
          AND typeof("{details["start_lat_col"]}") NOT IN ('VARCHAR', 'NULL')
          AND typeof("{details["end_lng_col"]}") NOT IN ('VARCHAR', 'NULL')
          AND typeof("{details["end_lat_col"]}") NOT IN ('VARCHAR', 'NULL');
        """
        try:
            db_connection.execute(add_geom_query)
            print(f"Added geometry to {table_with_geom_name}")
        except Exception as e:
            print(f"Error adding geometry to {combined_name}: {str(e)}\nQuery: {add_geom_query}")
            continue
        
        count_geom_check = db_connection.execute(f'SELECT COUNT(*) FROM "{table_with_geom_name}"').fetchone()
        if count_geom_check and count_geom_check[0] == 0:
            print(f"Table with geometry {table_with_geom_name} is empty. Skipping Parquet export.")
            continue


        parquet_file_path = os.path.join(output_parquet_base_dir, f'{table_with_geom_name}.parquet')
        export_query = f"""
        COPY (
            SELECT * FROM "{table_with_geom_name}" WHERE year IS NOT NULL AND month IS NOT NULL
        ) TO '{parquet_file_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE)
        """
        try:
            db_connection.execute(export_query)
            print(f"Exported {table_with_geom_name} to Parquet at {parquet_file_path}")
        except Exception as e:
            print(f"Error exporting {table_with_geom_name} to Parquet: {str(e)}\nQuery: {export_query}")

# Main execution logic
if __name__ == "__main__":
    START_YEAR = 2023 # Test with a year known to have direct CSVs in zips for simplicity first
    END_YEAR = 2023
    END_MONTH_FOR_FINAL_YEAR = 1 # Jan 2023 for testing

    # To test nested zips, you might need a specific year/month known to have them,
    # e.g., if 2016 data had zips inside zips.
    # For example:
    # START_YEAR = 2016
    # END_YEAR = 2016
    # END_MONTH_FOR_FINAL_YEAR = 12 # Full year

    TEMP_DOWNLOAD_DIR = "temp_citibike_data_main"
    DB_FILE = 'citibike_data_main.db'
    PARQUET_OUTPUT_DIR = 'final_parquet_output_main'

    if os.path.exists(TEMP_DOWNLOAD_DIR):
        shutil.rmtree(TEMP_DOWNLOAD_DIR)
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    if os.path.exists(PARQUET_OUTPUT_DIR):
        shutil.rmtree(PARQUET_OUTPUT_DIR)
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(PARQUET_OUTPUT_DIR, exist_ok=True)

    db_con = None
    try:
        db_con = duckdb.connect(database=DB_FILE, read_only=False)
        print(f"DuckDB connection established to {DB_FILE}.")
        print(f"DuckDB version: {db_con.execute('SELECT version()').fetchone()[0]}")


        file_urls_to_download = generate_file_names(START_YEAR, END_YEAR, END_MONTH_FOR_FINAL_YEAR)
        if not file_urls_to_download:
            print("No file URLs generated. Exiting.")
        else:
            print(f"Generated {len(file_urls_to_download)} URLs to download: {file_urls_to_download}")
            
            print(f"\nStarting download, extraction, and processing CSVs into DuckDB...")
            csv_processed_count = 0
            for csv_file_path in download_and_extract_files_generator(file_urls_to_download, TEMP_DOWNLOAD_DIR):
                print(f"\nMAIN LOOP: Generator yielded --> {csv_file_path}")
                if not os.path.exists(csv_file_path): # Defensive check
                    print(f"Warning: CSV file path yielded by generator does not exist: {csv_file_path}. Skipping.")
                    continue
                process_csv_to_duckdb(csv_file_path, db_con)
                csv_processed_count += 1
                # Optionally delete the individual CSV file after it's processed to save disk space,
                # but ensure it's not needed by a later step if you do.
                # try:
                #     os.remove(csv_file_path)
                #     print(f"Deleted processed CSV: {csv_file_path}")
                # except OSError as e_del_csv:
                #     print(f"Error deleting processed CSV {csv_file_path}: {e_del_csv}")

            print(f"\nFinished processing loop. Total CSVs processed into DuckDB: {csv_processed_count}")

            if csv_processed_count > 0:
                print("\nStarting Parquet conversion process...")
                convert_parquet(db_con, PARQUET_OUTPUT_DIR)
                print("Parquet conversion process complete.")
            else:
                print("No CSVs were processed, skipping Parquet conversion.")

    except Exception as e:
        print(f"An error occurred in the main execution block: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if db_con:
            db_con.close()
            print("DuckDB connection closed.")
        print("Script execution finished.")