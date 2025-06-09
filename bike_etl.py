!pip install wget
!pip install zipfile
!pip install --quiet jupysql
# !pip install --quiet duckdb-engine
# !pip install leafmap
# !pip install lonboard
# !pip install h3
# !pip install keplergl
# !pip install duckdb==1.0.0
import os
import pandas as pd
import geopandas as gpd
import wget
import zipfile
from datetime import datetime, timedelta
import re
#import leafmap
#import lonboard
#import h3
import requests
import duckdb
print(f"DuckDB version according to Python: {duckdb.__version__}")


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
    local_file_list = []  # Initialize file_list locally

    for year_iter in range(start_year_param, end_year_param + 1):
        # Determine the number of months to iterate for the current year_iter.
        # If it's not the end_year_param, we process all 12 months (for years >= 2024 that are not the final year).
        # If it is the end_year_param, we process up to end_month_for_final_year_param.
        num_months_to_iterate = 12
        if year_iter == end_year_param:
            num_months_to_iterate = end_month_for_final_year_param

        if year_iter < 2024:
            # Annual zip files for years before 2024
            # Example: https://s3.amazonaws.com/tripdata/2013-citibike-tripdata.zip
            file_name = f"{base_url}{year_iter}-citibike-tripdata.zip"
            local_file_list.append(file_name)
        elif year_iter == 2024:
            # Monthly files for 2024
            for month_iter in range(1, num_months_to_iterate + 1):
                # For 2024, months May (5) to October (10) are '.zip'
                # Other months (Jan-Apr, Nov-Dec) are '.csv.zip'
                if month_iter in [5, 6, 7, 8, 9, 10,11,12]:
                    # Example: https://s3.amazonaws.com/tripdata/202405-citibike-tripdata.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                else:
                    # Example: https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                local_file_list.append(file_name)
        elif year_iter == 2025:
            # Monthly files for 2025
            # As per new rules: March (3) is '.csv.zip', all other months are '.zip'
            for month_iter in range(1, num_months_to_iterate + 1):
                if month_iter == 3:  # March 2025
                    # Example: https://s3.amazonaws.com/tripdata/202503-citibike-tripdata.csv.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.csv.zip"
                else:
                    # Example: https://s3.amazonaws.com/tripdata/202501-citibike-tripdata.zip
                    file_name = f"{base_url}{year_iter}{month_iter:02d}-citibike-tripdata.zip"
                local_file_list.append(file_name)
        # else:
            # If end_year_param could go beyond 2025 and new rules are defined for those future years,
            # additional elif blocks for those years would be added here.
            # For now, rules are specified up to 2025.

    return local_file_list
start_year = 2013
end_year = 2025
end_month = 4 # For the final year (2025), process up to April

print("Generating file list...")
files_to_download = generate_file_names(start_year, end_year, end_month)

files_to_download




def download_file(url_list, destination_folder):
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for url in url_list:
        try:
            # Step 1: Download the file using wget
            filename = wget.download(url, out=destination_folder)
            print(f"\nFile downloaded: {filename}")

            # Step 2: Unzip the file
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
            print(f"File unzipped: {filename}")

            # Step 3: Delete the zip file
            os.remove(filename)
            print(f"Zip file deleted: {filename}")
        except Exception as e:
            print(f"Error processing URL: {url}")
            print(f"Error message: {str(e)}")
    #remove macosx file
    macosx_folder = os.path.join(destination_folder, '__MACOSX')
    if os.path.exists(macosx_folder):
        for root, dirs, files in os.walk(macosx_folder, topdown=False):
              for name in files:
                    os.remove(os.path.join(root, name))
              for name in dirs:
                    os.rmdir(os.path.join(root, name))
        os.rmdir(macosx_folder)
        print(f"Removed folder: {macosx_folder}")
download_file(files_to_download,'/content/citi_data')


def unzip_nested_zip_files(root_directory):
    for root, dirs, files in os.walk(root_directory):
        for filename in files:
            if filename.endswith('.zip'):
                zip_file_path = os.path.join(root, filename)

                # Unzip the file
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(root)

                # Delete the zip file
                os.remove(zip_file_path)
                print(f"Unzipped and deleted: {zip_file_path}")

# Usage
unzip_nested_zip_files('/content/citi_data')

def standardize_and_store_data(root_directory, db_file='citibike_data.db'):
    con = duckdb.connect(db_file)

    for root, dirs, files in os.walk(root_directory):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                # Modified regex to capture both year and month
                date_match = re.search(r'(20\d{2})(\d{2})', filename)
                if date_match:
                    year, month = date_match.groups()
                    table_name = f"citibike_data_{year}_{month}"

                    sample_query = f"SELECT * FROM read_csv_auto('{file_path}', header=true, sample_size=-1) LIMIT 1"
                    sample_df = con.execute(sample_query).fetchdf()

                    if 'member_casual' in sample_df.columns:
                        # Schema for newer files
                        insert_query = f"""
                        CREATE OR REPLACE TABLE {table_name}_new_schema AS
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
                        FROM read_csv('{file_path}', header=true, types={{'start_station_id': 'VARCHAR', 'end_station_id': 'VARCHAR'}})
                        """

                    elif 'gender' or 'Gender' in sample_df.columns:
                        # Schema for older files #couple ones with uppercase
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


                        if start_time_col=='starttime' and sample_df['starttime'].dtype!='<M8[us]':    #string format: 11/1/2014 00:01:48

                          insert_query = f"""
                              CREATE OR REPLACE TABLE {table_name}_old_schema AS
                              SELECT
                                 CASE
                                      WHEN "{start_time_col}" ~ '^\d{{1,2}}/\d{{1,2}}/\d{{4}} \d{{1,2}}:\d{{1,2}}:\d{{1,2}}$' THEN strptime("{start_time_col}", '%m/%d/%Y %H:%M:%S')
                                      WHEN "{start_time_col}" ~ '^\d{{1,2}}/\d{{1,2}}/\d{{4}} \d{{1,2}}:\d{{1,2}}$' THEN strptime("{start_time_col}", '%m/%d/%Y %H:%M')
                                      ELSE NULL
                                      END AS starttime,
                                  CASE
                                      WHEN "{stop_time_col}" ~ '^\d{{1,2}}/\d{{1,2}}/\d{{4}} \d{{1,2}}:\d{{1,2}}:\d{{1,2}}$' THEN strptime("{stop_time_col}", '%m/%d/%Y %H:%M:%S')
                                      WHEN "{stop_time_col}" ~ '^\d{{1,2}}/\d{{1,2}}/\d{{4}} \d{{1,2}}:\d{{1,2}}$' THEN strptime("{stop_time_col}", '%m/%d/%Y %H:%M')
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
                                  CASE WHEN "{birth_year_col}" IS NOT NULL THEN LEFT(CAST("{birth_year_col}" as VARCHAR),4) ELSE NULL END AS birth_year,
                                  "{gender_col}"::BIGINT AS gender
                              FROM read_csv('{file_path}', header=true, types={{'start station id': 'VARCHAR', 'end station id':'VARCHAR','starttime':'VARCHAR','stoptime':'VARCHAR'}})
                              """

                        else: #cases when all starttime col is timestamp

                          insert_query = f"""
                        CREATE OR REPLACE TABLE {table_name}_old_schema AS
                        SELECT
                            "{start_time_col}" ::TIMESTAMP AS starttime,
                            "{stop_time_col}" ::TIMESTAMP AS stoptime,
                            "{start_station_id_col}"::VARCHAR AS start_station_id,
                            "{start_station_name_col}" AS start_station_name,
                            "{start_station_lat_col}"::DOUBLE AS start_station_latitude,
                            "{start_station_lng_col}"::DOUBLE AS start_station_longitude,
                            "{end_station_id_col}"::VARCHAR AS end_station_id,
                            "{end_station_name_col}" AS end_station_name,
                            NULLIF("{end_station_lat_col}", 'NULL')::DOUBLE AS end_station_latitude,
                            NULLIF("{end_station_lng_col}", 'NULL')::DOUBLE AS end_station_longitude,
                            "{bikeid_col}"::BIGINT AS bikeid,
                            "{usertype_col}" AS usertype,
                            CASE WHEN "{birth_year_col}" IS NOT NULL THEN LEFT(CAST("{birth_year_col}" as VARCHAR),4) ELSE NULL END AS birth_year,
                            "{gender_col}"::BIGINT AS gender
                        FROM read_csv('{file_path}', header=true, types={{'start station id': 'VARCHAR', 'end station id':'VARCHAR', 'end station latitude':'VARCHAR', 'end station longitude':'VARCHAR'}})
                        """

                    else:
                        print(f"Unknown schema for file: {filename}")
                        continue

                    try:

                        con.execute(insert_query)
                        print(f"Processed and stored data from {filename} into {table_name}")
                    except Exception as e:
                        print(f"Error processing {table_name}: {str(e)}")
                else:
                    print(f"Could not extract date from filename: {filename}")

                    # Error processing citibike_data_2014_10: Parser Error: syntax error at or near ":"
                    # Error processing citibike_data_2014_09: Parser Error: syntax error at or near ":"

    con.close()

# Usage
standardize_and_store_data('/content/citi_data')



def convert_parquet(db_file='citibike_data.db'):
    con = duckdb.connect(db_file)

    # Get the list of all tables
    tables = con.execute('SHOW TABLES').fetchdf()

    # Initialize lists to hold table names for each schema type
    old_schema_tables = []
    new_schema_tables = []

    # Classify tables by schema type
    for table_name in tables['name']:
        if '_old_schema' in table_name:
            old_schema_tables.append(table_name)
        elif '_new_schema' in table_name:
            new_schema_tables.append(table_name)

    # Combine old schema tables
    if old_schema_tables:
        old_schema_combined_query = f"""
        CREATE OR REPLACE TABLE old_schema_combined AS
        SELECT *
        FROM {' UNION ALL SELECT * FROM '.join(old_schema_tables)}
        """
        print("Old Schema Combined Query:")
        print(old_schema_combined_query)  # Print the query to see what it looks like
        con.execute(old_schema_combined_query)
        add_geometry_and_export(con, 'old_schema_combined', 'start_station_longitude', 'start_station_latitude', 'end_station_longitude', 'end_station_latitude', 'starttime')

    # Combine new schema tables
    if new_schema_tables:
        new_schema_combined_query = f"""
        CREATE OR REPLACE TABLE new_schema_combined AS
        SELECT *
        FROM {' UNION ALL SELECT * FROM '.join(new_schema_tables)}
        """
        print("New Schema Combined Query:")
        print(new_schema_combined_query)  # Print the query to see what it looks like
        con.execute(new_schema_combined_query)
        add_geometry_and_export(con, 'new_schema_combined', 'start_lng', 'start_lat', 'end_lng', 'end_lat', 'started_at')

    con.close()

def add_geometry_and_export(con, table_name, start_lng_col, start_lat_col, end_lng_col, end_lat_col, time_col):
     # Install and load spatial extension
    con.install_extension("spatial")
    con.load_extension("spatial")
    # Add geometry columns
    query = f"""
    CREATE OR REPLACE TABLE {table_name}_with_geom AS
    SELECT *,
           st_aswkb(st_point("{end_lng_col}", "{end_lat_col}"))::BLOB AS end_geom,
           st_aswkb(st_point("{start_lng_col}", "{start_lat_col}"))::BLOB AS start_geom,
           YEAR("{time_col}") AS year,
           MONTH("{time_col}") AS month
    FROM {table_name}
    """
    con.execute(query)

    #Export the table to Parquet, partitioned by year and month
    parquet_file_path = f'/content/final_parquet/{table_name}_with_geom.parquet'
    export_query = f"""
    COPY (
        SELECT * FROM {table_name}_with_geom
    ) TO '{parquet_file_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month))
    """
    con.execute(export_query)

    print(f"Parquet file created for table {table_name} at {parquet_file_path}")

# Usage
convert_parquet('/content/citibike_data.db')