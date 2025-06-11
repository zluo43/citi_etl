# %%
import duckdb
import pandas as pd
import os

# %%
# Database Connection Setup
con = duckdb.connect(':memory:')

# %%
# Install and load spatial extension

con.install_extension("spatial")
con.load_extension("spatial")


# %%
con.sql("""

SELECT start_geom,start_geom_bbox FROM read_parquet('/Users/zluo43/Desktop/citi_etl/citi_etl/final_parquet_output/new_schema_combined_with_geom.parquet/year=2023/month=12/data_0.parquet')
""")
con.close()


# %%
