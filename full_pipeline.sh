#!/bin/bash

# --- CONFIGURATION ---
# All variables are filled in based on your requirements.

# Part 1: Python ETL Script Configuration
# NOTE: Make sure your Python script is named 'improved_etl.py' or change this value.
PY_SCRIPT="improved_etl.py"
START_YEAR="2013"
END_YEAR="2014"
END_MONTH="12"
DB_FILE="citibike_data.db"
TEMP_DIR="temp_citibike_data"
PARQUET_DIR="final_parquet_folder"

# Part 2: GDAL Conversion Script Configuration
# NOTE: Make sure your GDAL script is named 'convert_to_geoparquet.sh' or change this value.
GDAL_SCRIPT="convert_parquet.sh"

# Part 3: Source Cooperative Upload Configuration
# Set to 'default' as per your existing AWS CLI setup.
SOURCE_COOP_PROFILE="default"
# Set to the S3 path you provided.
SOURCE_COOP_PATH="s3://zluo43/citibike/"


# --- PIPELINE EXECUTION ---

# Use 'set -e' to make the script exit immediately if any command fails.
set -e

echo "--- STARTING FULL DATA PIPELINE ---"
echo "Start Year: $START_YEAR, End Year: $END_YEAR, End Month: $END_MONTH"
echo "Final Output will be uploaded to: $SOURCE_COOP_PATH"

# Step 1: Run the Python ETL script to create the initial Parquet files.
echo
echo "--> STEP 1: Running Python ETL script..."
python3 "$PY_SCRIPT" \
    --start-year "$START_YEAR" \
    --end-year "$END_YEAR" \
    --end-month "$END_MONTH" \
    --db-file "$DB_FILE" \
    --temp-dir "$TEMP_DIR" \
    --output-dir "$PARQUET_DIR"
echo "--> STEP 1: Python ETL complete."


# Step 2: Run the GDAL shell script to convert Parquet to GeoParquet.
echo
echo "--> STEP 2: Running GDAL script to add CRS and BBox..."
# First, ensure the GDAL script has execute permissions
chmod +x "$GDAL_SCRIPT"
# Now, run it
./"$GDAL_SCRIPT"
echo "--> STEP 2: GeoParquet conversion complete."


# Step 3: Upload the final GeoParquet data to Source Cooperative.
# This syncs ONLY the contents of your final output directory.
echo
echo "--> STEP 3: Uploading final data from '$PARQUET_DIR' to Source Cooperative..."
aws s3 sync "$PARQUET_DIR" "$SOURCE_COOP_PATH" --endpoint-url https://data.source.coop --profile "$SOURCE_COOP_PROFILE"
echo "--> STEP 3: Upload complete."


echo
echo "--- PIPELINE FINISHED SUCCESSFULLY ---"