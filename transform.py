import re

from pyspark.sql.functions import lit, current_timestamp

from pyspark.sql.types import StructField, StructType, StringType

from utils import list_existing_parquet_folders, make_unique_columns

 

def coerce_row(row):

    # Convert all values to strings; None stays None

    return tuple(str(cell) if cell is not None else None for cell in row)

 

def process_sheet(spark, input_path, output_path, config):

    # Sanitize sheet name for folder naming

    safe_sheet = re.sub(r'[^a-zA-Z0-9_]', '_', config["sheet_name"])

   

    # Skip processing if sheet already converted

    existing_sheets = list_existing_parquet_folders(output_path)

    if safe_sheet in existing_sheets:

        print(f" Skipping already processed sheet: {config['sheet_name']}")

        return

 

    print(f" Processing: {config['sheet_name']} (Header from row {config['header_row_index']}, data from row {config['data_start_row_index']})")

 

    # Read full sheet starting at header_row_index

    full_df = (

        spark.read.format("com.crealytics.spark.excel")

        .option("header", "false")

        .option("inferSchema", "false")

        .option("dataAddress", f"'{config['sheet_name']}'!A{config['header_row_index']}")

        .load(input_path)

    )

 

    # Collect rows for header + data slicing

    rows = full_df.collect()

    if len(rows) < 2:

        print(" Not enough rows to extract header and data.")

        return

 

    # Extract header and data

    header_row_data = rows[0]

    data_rows = rows[1:]

 

    # Clean and deduplicate column names

    raw_headers = [str(cell).strip() if cell is not None else "col" for cell in header_row_data]

    final_headers = make_unique_columns(raw_headers)

 

    # Coerce data rows to string type for consistency

    data = [coerce_row(row) for row in data_rows]

    schema = StructType([StructField(col, StringType(), True) for col in final_headers])

 

    try:

        df = spark.createDataFrame(data, schema=schema)

    except Exception as e:

        print(f" Failed to create DataFrame with explicit schema: {e}")

        return

 

    # Show column remapping

    print(" Column Mapping:")

    for raw, clean in zip(raw_headers, final_headers):

        if raw != clean:

            print(f" - {raw} → {clean}")

 

    # Add metadata columns

    df = df.withColumn("source_file_name", lit(input_path)) \

           .withColumn("sheet_name", lit(config["sheet_name"])) \

           .withColumn("ingestion_timestamp", current_timestamp())

 

    if df.count() > 0:

        output_folder = f"{output_path}/{safe_sheet}"

        df.coalesce(1).write.mode("overwrite") \

            .option("header", "true") \

            .option("parquet.enable.summary-metadata", "true") \

            .parquet(output_folder)

        print(f"  Saved to: {output_folder}")

    else:

        print(f" Sheet '{config['sheet_name']}' has no valid rows.")
