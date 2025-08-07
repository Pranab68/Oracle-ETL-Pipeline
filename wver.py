
import re

import html
import oci
def extract_oci_info(oci_path):

    """

    Extract bucket, namespace, and prefix from an OCI URI.

    Example: oci://my-bucket@namespace-name/path/to/folder

    """

    match = re.match(r"oci://([^@]+)@([^/]+)/(.+)", oci_path)

    if not match:

        raise ValueError(f"Invalid OCI path: {oci_path}")

    return match.group(1), match.group(2), match.group(3)

 

 

def list_existing_parquet_folders(output_path):

    """

    List existing folder prefixes in the given OCI Object Storage output path.

    Used to skip already-processed Excel sheets.

    """

    try:

        signer = oci.auth.signers.get_resource_principals_signer()

        object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)

        bucket, namespace, prefix = extract_oci_info(output_path)

        prefix = prefix.rstrip("/") + "/"

 

        response = object_storage.list_objects(namespace, bucket, prefix=prefix, delimiter="/")

        return set([

            html.unescape(p.rstrip("/").split("/")[-1])

            for p in response.data.prefixes

        ])

    except Exception as e:

        print(f"Failed to list existing parquet folders: {e}")

        return set()

 

 

def make_unique_columns(columns):

    """

    1. Trim whitespace, replace non‑alphanumerics with '_'

    2. Lower‑case everything (optional but avoids 'Name' vs 'name')

    3. Replace blanks/None with 'col'

    4. Deduplicate with numeric suffixes _1, _2 ...

    """

    seen = {}

    unique = []

 

    for raw in columns:

        # 1‑2. normalise

        col = "" if raw is None else str(raw).strip()

        col = re.sub(r"[^\w]", "_", col)         # keep A‑Z a‑z 0‑9 _

        col = col.lower()                        # comment this out if you need case

 

        # 3. handle empty after cleaning

        if not col:

            col = "col"

 

        # 4. dedupe

        base = col

        count = seen.get(base, 0)

        new_col = base if count == 0 else f"{base}_{count}"

        while new_col in seen:

            count += 1

            new_col = f"{base}_{count}"

 

        unique.append(new_col)

        seen[base] = count + 1

        seen[new_col] = 1      # mark the generated name too

 

    return unique
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

 

import sys

import traceback

import os

import yaml

from pyspark.sql import SparkSession

from pyspark import SparkFiles

from pyspark.context import SparkContext

 

 

# ---------------------------------------------------

# helpers

# ---------------------------------------------------

def in_dataflow() -> bool:

    """Detect if we are inside OCI Data Flow"""

    return os.environ.get("HOME") == "/home/dataflow"

 

 

# ---------------------------------------------------

# resolve code & config paths

# ---------------------------------------------------

local = 0 if in_dataflow() else 1

 

if not local:

    if len(sys.argv) != 2:

        raise ValueError("Usage in Data Flow: main.py <code_config_path>")

    code_root = sys.argv[1]

else:

    code_root = "."

 

if not local:

    sc = SparkContext.getOrCreate()

    # ship supporting modules to executors

    sc.addPyFile(os.path.join(code_root, "transform.py"))

    sc.addPyFile(os.path.join(code_root, "utils.py"))

    sc.addFile(os.path.join(code_root, "config.yaml"))

    config_path = SparkFiles.get("config.yaml")

else:

    config_path = os.path.join(code_root, "config.yaml")

 

# ---------------------------------------------------

# load & validate config

# ---------------------------------------------------

with open(config_path, "r") as f:

    cfg_root = yaml.safe_load(f)

 

# allow legacy single‑sheet configs

sheet_configs = cfg_root.get("sheets", [cfg_root])

 

required = {"sheet_name", "header_row_index", "data_start_row_index",

            "input_path", "output_path"}

 

for i, sheet_cfg in enumerate(sheet_configs, start=1):

    missing = required.difference(sheet_cfg)

    if missing:

        raise KeyError(f"Sheet config #{i} missing keys: {sorted(missing)}")

 

# ---------------------------------------------------

# build Spark session once

# ---------------------------------------------------

builder = (

    SparkSession.builder

        .appName(cfg_root.get("app_name", "ExcelToParquetJob"))

        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        .config("spark.debug.maxToStringFields", "2000")

        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

)

 

if local and cfg_root.get("excel_jar_path"):

    builder = builder.config(

        "spark.jars",

        os.path.join(code_root, cfg_root["excel_jar_path"])

    )

 

spark = builder.getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark.conf.set("parquet.enable.summary-metadata", "false")

 

# ---------------------------------------------------

# run the ETL for each sheet

# ---------------------------------------------------

from transform import process_sheet

 

for sheet_cfg in sheet_configs:

    try:

        print(f'  Processing "{sheet_cfg["sheet_name"]}" from {sheet_cfg["input_path"]}')

        process_sheet(

            spark=spark,

            input_path=sheet_cfg["input_path"],

            output_path=sheet_cfg["output_path"],

            config=sheet_cfg,        # transform.py can pick what it needs

        )

        print(f' Finished "{sheet_cfg["sheet_name"]}"')

    except Exception as exc:

        print(f' Error in "{sheet_cfg["sheet_name"]}": {exc}')

        traceback.print_exc()

 

spark.stop()

 

app_name: "ExcelToParquet_SM_NEOM"

excel_jar_path: "spark-excel_2.12-3.3.1_0.18.7.jar"

 

sheets:

  - sheet_name: "FCL_DISTRIBUTIONBOX_AREA"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_EDSERVICEPOINT"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_LVOHELECTRICLINESEGMENT"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_LVUGELECTRICLINESEGMENT"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_MVOHELECTRICLINESEGMENT"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_MVUGELECTRICLINESEGMENT"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_RMU_AREA"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_STATION_AREA"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "FCL_SUBSTATION_AREA"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"

 

  - sheet_name: "OCL_METER"

    header_row_index: 1

    data_start_row_index: 2

    input_path: "oci://bkt-neom-enowa-drp-dev-data-landing@axjj8sdvrg1w/Asset/Bespoke GIS/Feature_Class/12.03_Neom_21May_FeatureClass_Count_Tables.xlsx"

    output_path: "oci://bkt-neom-enowa-drp-dev-data-raw@axjj8sdvrg1w/test666"
