# 🚴‍♂️ NYC Citi Bike Data ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing NYC Citi Bike trip data into cloud-native GeoParquet format, optimized for geospatial analysis and data science workflows.

## 📊 Dataset Description

This project processes the official New York City Citi Bike trip data from [Citi Bike System Data](https://citibikenyc.com/system-data) and transforms it into a standardized, cloud-native GeoParquet format partitioned by year and month.

**Key Features:**
- ✅ Handles schema inconsistencies across different time periods
- ✅ Standardizes data types and column names
- ✅ Adds geospatial geometry columns for start/end locations
- ✅ Partitioned by year/month for efficient querying
- ✅ Cloud-optimized GeoParquet format
- ✅ Automated download and processing pipeline

## 🏗️ Architecture

The pipeline consists of three main components:

1. **Data Extraction & Processing** (`improved_etl.py`) - Downloads, extracts, and processes CSV files
2. **GeoParquet Conversion** (`convert_parquet.sh`) - Adds CRS and bounding box metadata
3. **Cloud Upload** (`full_pipeline.sh`) - Orchestrates the entire pipeline and uploads to cloud storage

## 📋 Data Schema

The original Citi Bike data exhibits inconsistencies across source files. This pipeline standardizes the data into two distinct schemas:

### Old Schema (2013-2017)
- `starttime`, `stoptime` - Trip start/end timestamps
- `start_station_id`, `start_station_name` - Start station information
- `end_station_id`, `end_station_name` - End station information
- `bikeid` - Bike identifier
- `usertype` - Customer or Subscriber
- `birth_year`, `gender` - User demographics

### New Schema (2018+)
- `started_at`, `ended_at` - Trip start/end timestamps
- `start_station_id`, `start_station_name` - Start station information
- `end_station_id`, `end_station_name` - End station information
- `ride_id` - Trip identifier
- `rideable_type` - Bike type (classic, electric, etc.)
- `member_casual` - Member or casual user

Both schemas include:
- Latitude/longitude coordinates for start and end locations
- Generated geometry columns (`start_geom`, `end_geom`) for geospatial analysis

## 🚀 Quick Start

### Prerequisites

- Python
- DuckDB
- GDAL (for GeoParquet conversion)


## 📖 Usage

### Option 1: Run Complete Pipeline

```bash
# Edit configuration in full_pipeline.sh
./full_pipeline.sh
```

### Option 2: Run Individual Components

**Process specific date range:**
```bash
python improved_etl.py --start-year 2023 --end-year 2023 --end-month 6
```

**Convert to GeoParquet:**
```bash
./convert_parquet.sh
```

### Option 3: Interactive Analysis

Use the provided Jupyter-style notebook for interactive exploration:

```bash
python duckdb_cell.py
```

Or run individual cells in VS Code/PyCharm with the `# %%` cell delimiters.

## 📊 Data Access

### Load Processed Data

**With DuckDB (Python):**
```python
import duckdb

con = duckdb.connect(database=":memory:")

# Load old schema data (2013-2017)
old_data = con.sql("""
    SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/zluo43/citibike/old_schema_combined_with_geom.parquet/**/*.parquet')
    LIMIT 1000
""").df()

# Load new schema data (2018+)
new_data = con.sql("""
    SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/zluo43/citibike/new_schema_combined_with_geom.parquet/**/*.parquet')
    LIMIT 1000
""").df()
```

**With Pandas:**
```python
import pandas as pd

# Read specific year/month partition
df = pd.read_parquet('s3://us-west-2.opendata.source.coop/zluo43/citibike/old_schema_combined_with_geom.parquet/year=2014/month=10/')
```

**With GeoPandas (for spatial analysis):**
```python
import geopandas as gpd

# Load with geometry
gdf = gpd.read_parquet('path/to/geoparquet/file.parquet')
```

## 🛠️ Configuration

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--start-year` | Start year for data processing | 2023 |
| `--end-year` | End year for data processing | 2023 |
| `--end-month` | End month for final year | 1 |
| `--temp-dir` | Temporary directory for downloads | temp_citibike_data |
| `--db-file` | DuckDB database file | citibike_data.db |
| `--output-dir` | Output directory for Parquet files | final_parquet_output |

### Pipeline Configuration

Edit `full_pipeline.sh` to customize:
- Date ranges
- Output paths
- AWS/S3 settings
- File locations

## 🔧 Technical Details

### Data Processing Features

- **Automatic Schema Detection**: Handles both old and new Citi Bike data schemas
- **Date Format Handling**: Processes various timestamp formats using regex and `strptime()`
- **Memory Efficient**: Uses generator-based processing for large datasets
- **Error Handling**: Robust error handling with detailed logging
- **Geospatial Enhancement**: Adds PostGIS-compatible geometry columns

### Performance Optimizations

- **Partitioned Storage**: Data partitioned by year/month for efficient querying
- **Columnar Format**: Parquet format for fast analytical queries
- **Compression**: ZSTD compression for optimal storage
- **Streaming Processing**: Generator-based approach minimizes memory usage

## 📁 Project Structure

```
citi-bike-etl/
├── improved_etl.py          # Main ETL script
├── full_pipeline.sh         # Complete pipeline orchestration
├── convert_parquet.sh       # GeoParquet conversion script
├── duckdb_cell.py          # Interactive analysis notebook
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── bike_etl.py            # Legacy ETL script
├── citi_etl.py            # Alternative ETL implementation
└── test_date_range.py     # Date range testing utility
```



