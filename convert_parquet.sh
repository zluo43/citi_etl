#!/bin/bash

# --- CONFIGURATION ---
# Define the parent folder where the output directories are located.
OUTPUT_PARENT_FOLDER="final_parquet_folder"

# Define an array containing the names of the two possible sub-directories.
# The script will loop through this array.
SCHEMA_FOLDERS=(
    "new_schema_combined_with_geom.parquet"
    "old_schema_combined_with_geom.parquet"
)


# --- SCRIPT LOGIC ---
echo "--- Starting Bulk Conversion to GeoParquet ---"

# Create an outer loop to process each schema folder defined above.
for schema_folder in "${SCHEMA_FOLDERS[@]}"; do
    
    # Construct the full path to the directory to be processed.
    PARQUET_ROOT="$OUTPUT_PARENT_FOLDER/$schema_folder"

    # First, check if this directory actually exists. If not, skip it.
    if [ ! -d "$PARQUET_ROOT" ]; then
        echo
        echo "--> Directory not found, skipping: $PARQUET_ROOT"
        continue # Go to the next item in the SCHEMA_FOLDERS array
    fi

    echo
    echo ">>> Processing all files in root directory: $PARQUET_ROOT"

    # Find every individual .parquet file within the current directory and loop through them.
    find "$PARQUET_ROOT" -type f -name "*.parquet" | while read original_file; do
        echo "--------------------------------------------------"
        echo "Processing: $original_file"

        # Create a temporary file name.
        temp_file="$(dirname "$original_file")/temp_geoparquet.parquet"

        # Run the conversion, writing the corrected data to the temporary file.
        ogr2ogr -f Parquet \
                -a_srs "EPSG:4326" \
                -lco GEOMETRY_NAME=start_geom \
                "$temp_file" \
                "$original_file"

        # Check if the conversion was successful.
        if [ $? -eq 0 ]; then
            # If successful, replace the original file with the new one.
            mv "$temp_file" "$original_file"
            echo "Success. Original file has been updated to GeoParquet."
        else
            # If it failed, print an error and stop the entire script.
            echo "ERROR: ogr2ogr failed for $original_file. Aborting."
            rm -f "$temp_file" # Clean up the failed temp file
            exit 1
        fi
    done
done

echo
echo "--- Bulk Conversion Complete for All Directories ---"