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
