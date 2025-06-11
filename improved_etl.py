import os
import pandas as pd
import wget
import zipfile
from datetime import datetime, timedelta
import re
import requests
import duckdb
import shutil
import time
import argparse
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def generate_file_names(start_year_param, end_year_param, end_month_for_final_year_param):
    """
    Generates a list of Citi Bike data file URLs based on specified year and month ranges
    and known naming conventions.
    """
    base_url = "https://s3.amazonaws.com/tripdata/"
    local_file_list = []

    for year_iter in range(start_year_param, end_year_param + 1):
        num_months_to_iterate = 12
        if year_iter == end_year_param:
            num_months_to_iterate = end_month_for_final_year_param

        if year_iter < 2024:
            # Annual zip files for years before 2024
            file_name = f"{base_url}{year_iter}-citibike-tripdata.zip"
            local_file_list.append(file_name)
        elif year_iter == 2024:
            # Monthly files for 2024
            for month_iter in range(1, num_months_to_iterate + 1):
                if month_iter in [5, 6, 7, 8, 9, 10, 11, 12]:
                    # Example: https://s3.amazonaws.com/tripdata/202405-citibike-tripdata.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                else:
                    # Example: https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                local_file_list.append(file_name)
        elif year_iter == 2025:
            # Monthly files for 2025
            for month_iter in range(1, num_months_to_iterate + 1):
                if month_iter == 3:  # March 2025
                    # Example: https://s3.amazonaws.com/tripdata/202503-citibike-tripdata.csv.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                else:
                    # Example: https://s3.amazonaws.com/tripdata/202501-citibike-tripdata.zip, 202505-citibike-tripdata.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                local_file_list.append(file_name)
    
    return local_file_list

def download_and_extract_files_generator(url_list, destination_folder):
    """
    Generator function to download, extract files (including nested zips), and yield CSV paths.
    Handles single-level nesting of zip files.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        print(f"Created destination folder: {destination_folder}")

    for url in url_list:
        downloaded_zip_path = None
        try:
            start_time = time.time()
            print(f"Downloading: {url}")
            downloaded_zip_path = wget.download(url, out=destination_folder)
            print(f"\nFile downloaded: {downloaded_zip_path} in {time.time() - start_time:.2f} seconds")
            
            extract_time = time.time()
            # Track all CSV files from this zip (including those from nested zips)
            csv_files_from_current_zip = []
            
            with zipfile.ZipFile(downloaded_zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.startswith('__MACOSX/') or member.endswith('.DS_Store') or member.endswith('/'):
                        continue
                    
                    # Extract file from zip
                    zip_ref.extract(member, destination_folder)
                    extracted_path = os.path.join(destination_folder, member)
                    
                    # Check if extracted file is a nested zip
                    if extracted_path.lower().endswith('.zip'):
                        print(f"Found nested zip: {extracted_path}")
                        try:
                            with zipfile.ZipFile(extracted_path, 'r') as nested_zip:
                                for nested_member in nested_zip.namelist():
                                    if nested_member.startswith('__MACOSX/') or nested_member.endswith('.DS_Store') or nested_member.endswith('/'):
                                        continue
                                    
                                    nested_zip.extract(nested_member, destination_folder)
                                    nested_extracted_path = os.path.join(destination_folder, nested_member)
                                    
                                    if nested_extracted_path.lower().endswith('.csv'):
                                        csv_files_from_current_zip.append(nested_extracted_path)
                                        print(f"Extracted nested CSV: {nested_extracted_path}")
                            
                            # Delete the nested zip after extracting its contents
                            os.remove(extracted_path)
                            print(f"Deleted nested zip: {extracted_path}")
                        except zipfile.BadZipFile:
                            print(f"Error: Nested file {extracted_path} is not a valid zip")
                        except Exception as e_nested:
                            print(f"Error with nested zip {extracted_path}: {str(e_nested)}")
                    
                    # Check if extracted file is a CSV
                    elif extracted_path.lower().endswith('.csv'):
                        csv_files_from_current_zip.append(extracted_path)
                        print(f"Extracted CSV: {extracted_path}")
            
            print(f"Extraction completed in {time.time() - extract_time:.2f} seconds")
            
            # Yield each CSV file found
            for csv_file in csv_files_from_current_zip:
                yield csv_file
                
        except Exception as e:
            print(f"Error processing URL: {url}")
            print(f"Error message: {str(e)}")
        finally:
            # Clean up the downloaded zip file
            if downloaded_zip_path and os.path.exists(downloaded_zip_path):
                try:
                    os.remove(downloaded_zip_path)
                    print(f"Deleted zip file: {downloaded_zip_path}")
                except Exception as e:
                    print(f"Error deleting zip file {downloaded_zip_path}: {str(e)}")
            print("-" * 50)  # Separator line
    
    # Clean up __MACOSX folder if it exists
    macosx_folder = os.path.join(destination_folder, '__MACOSX')
    if os.path.exists(macosx_folder) and os.path.isdir(macosx_folder):
        try:
            shutil.rmtree(macosx_folder)
            print(f"Removed __MACOSX folder: {macosx_folder}")
        except Exception as e:
            print(f"Error removing __MACOSX folder: {str(e)}")

def process_csv_to_duckdb(csv_file_path, db_connection):
    """
    Processes a single CSV file, standardizes its schema, and loads it into DuckDB.
    Handles multiple files for the same month by checking if a table already exists.
    Uses the original schema handling logic from bike_etl.py.
    """
    filename = os.path.basename(csv_file_path)
    process_start_time = time.time()
    print(f"Processing CSV: {filename}")

    # Extract year and month from filename using regex - simple pattern matching YYYYMM
    date_match = re.search(r'(20\d{2})(\d{2})', filename)
    
    if date_match:
        year_str, month_str = date_match.groups()
        table_name_suffix = f"{year_str}_{month_str}"
    else:
        # Generic fallback if no date match found
        generic_name = re.sub(r'[^a-zA-Z0-9_]', '_', os.path.splitext(filename)[0])
        table_name_suffix = f"unknown_date_{generic_name}"
        print(f"Could not extract year/month from filename: {filename}. Using suffix: {table_name_suffix}")

    try:
        # Sample query to determine schema
        sample_query = f"SELECT * FROM read_csv_auto('{csv_file_path}', header=true, sample_size=100, ignore_errors=true) LIMIT 1"
        sample_df = db_connection.execute(sample_query).fetchdf()
    except Exception as e:
        print(f"Error reading sample from {filename}: {str(e)}. Skipping file.")
        return

    if sample_df.empty:
        print(f"Sample from {filename} is empty or could not be read. Skipping file.")
        return

    # Check if a table for this month already exists
    table_exists_query = "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE ?"
    
    # Using your original schema detection and handling logic
    if 'member_casual' in sample_df.columns:
        # Schema for newer files
        final_table_name = f"citibike_data_{table_name_suffix}_new_schema"
        
        # Check if table exists
        table_exists = db_connection.execute(table_exists_query, [final_table_name]).fetchone()[0] > 0
        
        if table_exists:
            # Append to existing table
            print(f"Table {final_table_name} already exists. Appending data from {filename}.")
            insert_query = f"""
            INSERT INTO "{final_table_name}"
            SELECT
                COALESCE("ride_id") AS ride_id,
                COALESCE("rideable_type") AS rideable_type,
                COALESCE("started_at")::TIMESTAMP AS started_at,
                COALESCE("ended_at")::TIMESTAMP AS ended_at,
                COALESCE("start_station_name") AS start_station_name,
                COALESCE("start_station_id")::VARCHAR AS start_station_id,
                COALESCE("end_station_name") AS end_station_name,
                COALESCE("end_station_id")::VARCHAR AS end_station_id,
                COALESCE("start_lat")::DOUBLE AS start_lat,
                COALESCE("start_lng")::DOUBLE AS start_lng,
                COALESCE("end_lat")::DOUBLE AS end_lat,
                COALESCE("end_lng")::DOUBLE AS end_lng,
                COALESCE("member_casual") AS member_casual
            FROM read_csv('{csv_file_path}', header=true, types={{'start_station_id': 'VARCHAR', 'end_station_id': 'VARCHAR'}})
            """
        else:
            # Create new table - original logic
            insert_query = f"""
            CREATE TABLE "{final_table_name}" AS
            SELECT
                COALESCE("ride_id") AS ride_id,
                COALESCE("rideable_type") AS rideable_type,
                COALESCE("started_at")::TIMESTAMP AS started_at,
                COALESCE("ended_at")::TIMESTAMP AS ended_at,
                COALESCE("start_station_name") AS start_station_name,
                COALESCE("start_station_id")::VARCHAR AS start_station_id,
                COALESCE("end_station_name") AS end_station_name,
                COALESCE("end_station_id")::VARCHAR AS end_station_id,
                COALESCE("start_lat")::DOUBLE AS start_lat,
                COALESCE("start_lng")::DOUBLE AS start_lng,
                COALESCE("end_lat")::DOUBLE AS end_lat,
                COALESCE("end_lng")::DOUBLE AS end_lng,
                COALESCE("member_casual") AS member_casual
            FROM read_csv('{csv_file_path}', header=true, types={{'start_station_id': 'VARCHAR', 'end_station_id': 'VARCHAR'}})
            """
    elif 'gender' in sample_df.columns or 'Gender' in sample_df.columns:
        # Schema for older files - using your original logic
        final_table_name = f"citibike_data_{table_name_suffix}_old_schema"
        
        # Check if table exists
        table_exists = db_connection.execute(table_exists_query, [final_table_name]).fetchone()[0] > 0
        
        # Use your original column naming approach
        start_time_col = 'starttime' if 'starttime' in sample_df.columns else 'Start Time'
        stop_time_col = 'stoptime' if 'stoptime' in sample_df.columns else 'Stop Time'
        start_station_id_col = 'start station id' if 'start station id' in sample_df.columns else 'Start Station ID'
        start_station_name_col = 'start station name' if 'start station name' in sample_df.columns else 'Start Station Name'
        start_station_lat_col = 'start station latitude' if 'start station latitude' in sample_df.columns else 'Start Station Latitude'
        start_station_lng_col = 'start station longitude' if 'start station longitude' in sample_df.columns else 'Start Station Longitude'
        end_station_id_col = 'end station id' if 'end station id' in sample_df.columns else 'End Station ID'
        end_station_name_col = 'end station name' if 'end station name' in sample_df.columns else 'End Station Name'
        end_station_lat_col = 'end station latitude' if 'end station latitude' in sample_df.columns else 'End Station Latitude'
        end_station_lng_col = 'end station longitude' if 'end station longitude' in sample_df.columns else 'End Station Longitude'
        bikeid_col = 'bikeid' if 'bikeid' in sample_df.columns else 'Bike ID'
        usertype_col = 'usertype' if 'usertype' in sample_df.columns else 'User Type'
        birth_year_col = 'birth year' if 'birth year' in sample_df.columns else 'Birth Year'
        gender_col = 'gender' if 'gender' in sample_df.columns else 'Gender'
        
        #Old schema contains bad datetime format

        query_logic = f"""
        SELECT
            CASE
                WHEN "{start_time_col}" LIKE '%-%-%' THEN strptime(NULLIF(TRIM("{start_time_col}"), ''), '%Y-%m-%d %H:%M:%S')
                WHEN "{start_time_col}" LIKE '%/%/%' THEN strptime(NULLIF(TRIM("{start_time_col}"), ''), '%m/%d/%Y %H:%M:%S')
                ELSE NULL
            END AS starttime,
            CASE
                WHEN "{stop_time_col}" LIKE '%-%-%' THEN strptime(NULLIF(TRIM("{stop_time_col}"), ''), '%Y-%m-%d %H:%M:%S')
                WHEN "{stop_time_col}" LIKE '%/%/%' THEN strptime(NULLIF(TRIM("{stop_time_col}"), ''), '%m/%d/%Y %H:%M:%S')
                ELSE NULL
            END AS stoptime,
            "{start_station_id_col}"::VARCHAR AS start_station_id,
            "{start_station_name_col}" AS start_station_name,
            "{start_station_lat_col}"::DOUBLE AS start_station_latitude,
            "{start_station_lng_col}"::DOUBLE AS start_station_longitude,
            "{end_station_id_col}"::VARCHAR AS end_station_id,
            "{end_station_name_col}" AS end_station_name,
            "{end_station_lat_col}"::DOUBLE AS end_station_latitude,
            "{end_station_lng_col}"::DOUBLE AS end_station_longitude,
            "{bikeid_col}"::BIGINT AS bikeid,
            "{usertype_col}" AS usertype,
            TRY_CAST(LEFT(CAST("{birth_year_col}" as VARCHAR), 4) AS INTEGER) AS birth_year,
            TRY_CAST("{gender_col}" AS BIGINT) AS gender
        FROM read_csv('{csv_file_path}',
                    header=true,
                    all_varchar=true,
                    ignore_errors=true)
        """
        
        if table_exists:
            insert_query = f'INSERT INTO "{final_table_name}" ({query_logic})'
        else:
            insert_query = f'CREATE TABLE "{final_table_name}" AS ({query_logic})'  
    else:
        print(f"Unknown schema for file: {filename} (sample columns: {sample_df.columns.tolist()}). Skipping.")
        return

    # Execute query
    try:
        query_start_time = time.time()
        db_connection.execute(insert_query)
        operation_type = "appended to" if table_exists else "created"
        print(f"Successfully {operation_type} {final_table_name} from {filename} in {time.time() - query_start_time:.2f} seconds")
    except Exception as e:
        print(f"Error executing query for {final_table_name} from {filename}: {str(e)}")
    
    print(f"Total processing time for {filename}: {time.time() - process_start_time:.2f} seconds")

def convert_parquet(db_connection, output_parquet_dir):
    """
    Combines tables in DuckDB by schema type, adds geometry, and exports to partitioned Parquet.
    """
    if not os.path.exists(output_parquet_dir):
        os.makedirs(output_parquet_dir)
        print(f"Created Parquet output directory: {output_parquet_dir}")

    try:
        db_connection.install_extension("spatial")
        db_connection.load_extension("spatial")
    except Exception as e:
        print(f"Error loading spatial extension: {e}. Geospatial operations might fail.")

    base_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
    actual_tables_df = db_connection.execute(base_tables_query).fetchdf()

    if actual_tables_df.empty:
        print("No base tables found in the database to convert to Parquet.")
        return

    actual_tables = actual_tables_df['table_name'].tolist()

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
            print(f"Error combining tables for {combined_name}: {str(e)}")
            continue

        table_with_geom_name = f"{combined_name}_with_geom"
        count_check = db_connection.execute(f'SELECT COUNT(*) FROM "{combined_name}"').fetchone()
        if count_check and count_check[0] == 0:
            print(f"Combined table {combined_name} is empty. Skipping geometry addition and Parquet export.")
            continue

        add_geom_query = f"""
        CREATE OR REPLACE TABLE "{table_with_geom_name}" AS
        SELECT *,
               st_point("{details["end_lng_col"]}", "{details["end_lat_col"]}") AS end_geom,
               st_point("{details["start_lng_col"]}", "{details["start_lat_col"]}") AS start_geom,
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
            print(f"Error adding geometry to {combined_name}: {str(e)}")
            continue
        
        count_geom_check = db_connection.execute(f'SELECT COUNT(*) FROM "{table_with_geom_name}"').fetchone()
        if count_geom_check and count_geom_check[0] == 0:
            print(f"Table with geometry {table_with_geom_name} is empty. Skipping Parquet export.")
            continue

        parquet_file_path = os.path.join(output_parquet_dir, f'{table_with_geom_name}.parquet')
        export_query = f"""
        COPY (
            SELECT * FROM "{table_with_geom_name}" WHERE year IS NOT NULL AND month IS NOT NULL
        ) TO '{parquet_file_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE, COMPRESSION ZSTD)
        """
        try:
            db_connection.execute(export_query)
            print(f"Exported {table_with_geom_name} to Parquet at {parquet_file_path}")
        except Exception as e:
            print(f"Error exporting {table_with_geom_name} to Parquet: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Citibike ETL Process with generator-based processing')
    parser.add_argument('--start-year', type=int, default=2023, help='Start year')
    parser.add_argument('--end-year', type=int, default=2023, help='End year')
    parser.add_argument('--end-month', type=int, default=1, help='End month for final year')
    parser.add_argument('--temp-dir', type=str, default="temp_citibike_data", help='Temp directory for downloads')
    parser.add_argument('--db-file', type=str, default="citibike_data.db", help='DuckDB database file')
    parser.add_argument('--output-dir', type=str, default="final_parquet_output", help='Output directory for Parquet files')
    
    args = parser.parse_args()
    
    # Configuration parameters from command line
    START_YEAR = args.start_year
    END_YEAR = args.end_year
    END_MONTH = args.end_month
    TEMP_DOWNLOAD_DIR = args.temp_dir
    DB_FILE = args.db_file
    PARQUET_OUTPUT_DIR = args.output_dir
    
    # Clean up existing files/directories
    if os.path.exists(TEMP_DOWNLOAD_DIR):
        shutil.rmtree(TEMP_DOWNLOAD_DIR)
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    if os.path.exists(PARQUET_OUTPUT_DIR):
        shutil.rmtree(PARQUET_OUTPUT_DIR)
    
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(PARQUET_OUTPUT_DIR, exist_ok=True)
    
    # Connect to DuckDB
    db_con = duckdb.connect(database=DB_FILE, read_only=False)
    print(f"DuckDB connection established to {DB_FILE}.")
    print(f"DuckDB version: {db_con.execute('SELECT version()').fetchone()[0]}")
    
    try:
        # Generate file list
        print(f"Generating file list for {START_YEAR}-{END_YEAR} (up to month {END_MONTH} for {END_YEAR})...")
        files_to_download = generate_file_names(START_YEAR, END_YEAR, END_MONTH)
        print(f"Generated {len(files_to_download)} URLs to download")
        
        # Download, extract, and process files
        print("\nStarting download, extraction, and processing...")
        processed_count = 0
        
        for csv_file_path in download_and_extract_files_generator(files_to_download, TEMP_DOWNLOAD_DIR):
            print(f"\nProcessing extracted CSV: {csv_file_path}")
            process_csv_to_duckdb(csv_file_path, db_con)
            processed_count += 1
            
            # Optionally delete the CSV after processing to save space
            try:
                os.remove(csv_file_path)
                print(f"Deleted processed CSV: {csv_file_path}")
            except Exception as e:
                print(f"Error deleting CSV {csv_file_path}: {str(e)}")
        
        print(f"\nFinished processing {processed_count} CSV files")
        
        # Convert to Parquet if any files were processed
        if processed_count > 0:
            print("\nStarting Parquet conversion...")
            convert_parquet(db_con, PARQUET_OUTPUT_DIR)
            print("Parquet conversion complete")
        else:
            print("No CSVs were processed, skipping Parquet conversion.")
            
    except Exception as e:
        print(f"An error occurred in the main execution: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'db_con' in locals() and db_con:
            db_con.close()
            print("DuckDB connection closed")
        print("Script execution finished.") 