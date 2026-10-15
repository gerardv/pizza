# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b58a5437-69fc-4ce4-939e-bb95fc82cd34",
# META       "default_lakehouse_name": "Taxi",
# META       "default_lakehouse_workspace_id": "1b47e79e-9cb3-420c-8316-492b02b75565",
# META       "known_lakehouses": [
# META         {
# META           "id": "b58a5437-69fc-4ce4-939e-bb95fc82cd34"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver: NYC TLC trips
# 
# Converts the bronze files in `Files/bronze/nyctaxi` into typed Delta tables:
# 
# | Table | Source | Load |
# |---|---|---|
# | `silver_trips` | `yellow/` + `green/` monthly parquet | per `taxi_type` + `source_month` (re-runnable, `replaceWhere`) |
# | `silver_taxi_zone` | `reference/taxi_zone_lookup.csv` | full overwrite |
# 
# **Schema drift in the source** (checked over 2022-01 .. 2025-12):
# - From 2023-02 several columns changed type (int64 -> int32, double -> int64).
# - `airport_fee` was renamed to `Airport_fee` in 2023-02; Spark is case-insensitive, so reading both
#   in one go with `mergeSchema` fails with a duplicate column.
# - `cbd_congestion_fee` exists from 2025-01; green `ehail_fee` is an all-null column until 2023-01.
# 
# Therefore every file is read on its own and mapped onto one fixed target schema.
# Data quality issues are **flagged, not removed** (`is_*` / `has_*` columns); filtering is a gold decision.

# PARAMETERS CELL ********************

start_month = "2022-01"   # first month to process (yyyy-MM)
month_count = 48          # number of months from start_month
taxi_types = "yellow,green"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE = "Files/bronze/nyctaxi"
TRIPS_TABLE = "silver_trips"
ZONE_TABLE = "silver_taxi_zone"
AMOUNT = "decimal(10,2)"

# TLC timestamps are New York wall-clock times without a time zone. Keep the session in UTC so the
# conversion to TIMESTAMP does not shift them.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Target column -> (candidate source columns, matched case-insensitively; target type).
# A column missing in a file (e.g. airport_fee for green) becomes NULL.
COLUMNS = [
    ("vendor_id",             ["VendorID"],                                       "int"),
    ("pickup_datetime",       ["tpep_pickup_datetime", "lpep_pickup_datetime"],   "timestamp"),
    ("dropoff_datetime",      ["tpep_dropoff_datetime", "lpep_dropoff_datetime"], "timestamp"),
    ("passenger_count",       ["passenger_count"],                                "int"),
    ("trip_distance",         ["trip_distance"],                                  "double"),
    ("ratecode_id",           ["RatecodeID"],                                     "int"),
    ("store_and_fwd_flag",    ["store_and_fwd_flag"],                             "string"),
    ("pickup_location_id",    ["PULocationID"],                                   "int"),
    ("dropoff_location_id",   ["DOLocationID"],                                   "int"),
    ("payment_type",          ["payment_type"],                                   "int"),
    ("trip_type",             ["trip_type"],                                      "int"),
    ("fare_amount",           ["fare_amount"],                                    AMOUNT),
    ("extra",                 ["extra"],                                          AMOUNT),
    ("mta_tax",               ["mta_tax"],                                        AMOUNT),
    ("tip_amount",            ["tip_amount"],                                     AMOUNT),
    ("tolls_amount",          ["tolls_amount"],                                   AMOUNT),
    ("ehail_fee",             ["ehail_fee"],                                      AMOUNT),
    ("improvement_surcharge", ["improvement_surcharge"],                          AMOUNT),
    ("congestion_surcharge",  ["congestion_surcharge"],                           AMOUNT),
    ("airport_fee",           ["airport_fee"],                                    AMOUNT),
    ("cbd_congestion_fee",    ["cbd_congestion_fee"],                             AMOUNT),
    ("total_amount",          ["total_amount"],                                   AMOUNT),
]
AMOUNT_COLUMNS = [name for name, _, dtype in COLUMNS if dtype == AMOUNT]
MAPPED_SOURCES = {src.lower() for _, sources, _ in COLUMNS for src in sources}


def month_range(start: str, count: int) -> list:
    year, month = map(int, start.split("-"))
    result = []
    for i in range(count):
        add_years, month_index = divmod(month - 1 + i, 12)
        result.append(f"{year + add_years}-{month_index + 1:02d}")
    return result


def read_trip_file(taxi_type: str, month: str) -> DataFrame:
    path = f"{BRONZE}/{taxi_type}/{month[:4]}/{taxi_type}_tripdata_{month}.parquet"
    df = spark.read.parquet(path)
    by_lower = {c.lower(): c for c in df.columns}

    unmapped = sorted(c for c in df.columns if c.lower() not in MAPPED_SOURCES)
    if unmapped:
        # New source columns are not loaded silently: extend COLUMNS when this shows up.
        print(f"WARNING {taxi_type} {month}: unmapped source columns {unmapped}")

    selected = [F.lit(taxi_type).alias("taxi_type")]
    for target, sources, dtype in COLUMNS:
        found = next((by_lower[s.lower()] for s in sources if s.lower() in by_lower), None)
        col = F.col(f"`{found}`") if found else F.lit(None)
        selected.append(col.cast(dtype).alias(target))

    return (
        df.select(*selected)
        .withColumn("is_pickup_in_source_month", F.date_format("pickup_datetime", "yyyy-MM") == F.lit(month))
        .withColumn("is_dropoff_before_pickup", F.col("dropoff_datetime") < F.col("pickup_datetime"))
        .withColumn("has_negative_amount", F.least(*[F.coalesce(F.col(c), F.lit(0)) for c in AMOUNT_COLUMNS]) < 0)
        .withColumn("source_file", F.lit(path))
        .withColumn("source_month", F.lit(month))
        .withColumn("ingested_at", F.current_timestamp())
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_trips

# CELL ********************

months = month_range(start_month, int(month_count))
loaded = {}   # taxi_type -> [months]
frames = []

for taxi_type in [t.strip() for t in taxi_types.split(",") if t.strip()]:
    for month in months:
        path = f"{BRONZE}/{taxi_type}/{month[:4]}/{taxi_type}_tripdata_{month}.parquet"
        if not notebookutils.fs.exists(path):
            # Not (yet) published by TLC or not ingested: skip, and leave existing silver rows alone.
            print(f"SKIP {taxi_type} {month}: {path} not found")
            continue
        frames.append(read_trip_file(taxi_type, month))
        loaded.setdefault(taxi_type, []).append(month)

if not frames:
    raise ValueError(f"No bronze trip files found for {taxi_types} {months[0]}..{months[-1]}")

trips = reduce(DataFrame.unionByName, frames)

# Replace exactly the (taxi_type, month) combinations that were loaded, in one atomic commit.
replace_where = " OR ".join(
    f"(taxi_type = '{t}' AND source_month IN ({', '.join(repr(m) for m in ms)}))"
    for t, ms in loaded.items()
)

if spark.catalog.tableExists(TRIPS_TABLE):
    trips.write.format("delta").mode("overwrite").option("replaceWhere", replace_where).saveAsTable(TRIPS_TABLE)
else:
    trips.write.format("delta").mode("overwrite").saveAsTable(TRIPS_TABLE)

print(f"{TRIPS_TABLE}: replaced {replace_where}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_taxi_zone

# CELL ********************

zones = (
    spark.read.option("header", True).csv(f"{BRONZE}/reference/taxi_zone_lookup.csv")
    .select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )
    .withColumn("ingested_at", F.current_timestamp())
)
zones.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(ZONE_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# Row counts of what this run loaded. Returned as notebook exit value so the orchestrator can check it.

# CELL ********************

conditions = F.expr(replace_where)
summary = (
    spark.table(TRIPS_TABLE).where(conditions)
    .groupBy("taxi_type", "source_month")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.when(~F.col("is_pickup_in_source_month"), 1).otherwise(0)).alias("pickup_outside_month"),
        F.sum(F.when(F.col("is_dropoff_before_pickup"), 1).otherwise(0)).alias("dropoff_before_pickup"),
        F.sum(F.when(F.col("has_negative_amount"), 1).otherwise(0)).alias("negative_amount"),
    )
    .orderBy("taxi_type", "source_month")
)
display(summary)

result = {
    "trips_rows": summary.agg(F.sum("rows")).first()[0],
    "files": sum(len(ms) for ms in loaded.values()),
    "zones_rows": spark.table(ZONE_TABLE).count(),
}
print(result)
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
