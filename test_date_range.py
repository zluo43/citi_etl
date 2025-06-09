from improved_etl import generate_file_names

# Test with a range including May 2025
START_YEAR = 2025
END_YEAR = 2025
END_MONTH = 5  # May 2025

urls = generate_file_names(START_YEAR, END_YEAR, END_MONTH)
print(f"Generated {len(urls)} URLs:")
for url in urls:
    print(url)

# Specifically check if May 2025 is included
may_2025_url = "https://s3.amazonaws.com/tripdata/202505-citibike-tripdata.zip"
if may_2025_url in urls:
    print("\nConfirmed: May 2025 URL is included in the results")
else:
    print("\nError: May 2025 URL is not included in the results") 