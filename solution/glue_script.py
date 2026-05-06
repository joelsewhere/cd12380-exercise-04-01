import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Read job arguments ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", 
    "table", 
    "landing_path",
    "ingested_date",
    # Add a new job argument naming which file format the landing files are in (csv/json/parquet)
    "source_format",
])

# ── Configure spark ───────────────────────────────────────────────────────────

# Initialize a spark context
sc = SparkContext()

# Wrap the spark context in a GlueContext
glueContext = GlueContext(sc)

# Store the glue context's spark_session to the variable `spark`
spark = glueContext.spark_session

# Create a Glue Job tracking object for run-level features: bookmarks, run
# state, metrics, and integration with the Glue console run history.
job = Job(glueContext)

# Initialize the Job with its name and command-line args. Wires up bookmarks
# so re-runs can resume from prior state, registers the run with Glue, and
# starts metrics collection. Pair with job.commit() at the end of the script.
job.init(args["JOB_NAME"], args)

# ── Set up locations ────────────────────────────────────────────────────────────

table = args["table"]
landing_path = args["landing_path"]
ICEBERG_TABLE = f"iceberg.raw.{table}"

# ── Read landing files ──────────────────────────────────────────────────────────

ingested_date = args["ingested_date"]

# Pull the source format from job arguments and normalize it to lowercase for matching
source_format = args["source_format"].lower()

# If the source is JSON-formatted, branch into the JSON reader
if source_format == "json":
    # Read the landing files as newline-delimited JSON (Spark infers schema natively).
    # For pretty-printed or single-object files, you'd add .option("multiLine", "true").
    df = spark.read.json(landing_path)
# Otherwise if the source is CSV, branch into the CSV reader
elif source_format == "csv":
    # Read the landing files as CSV, treating the first row as the header
    df = spark.read.option("header", "true").csv(landing_path)
# Otherwise if the source is Parquet, branch into the Parquet reader
elif source_format == "parquet":
    # Read the landing files as Parquet (schema is embedded in the files, no reader options needed)
    df = spark.read.parquet(landing_path)
# Otherwise the format is unsupported — fail fast so misconfigured jobs surface clearly in logs
else:
    # Raise a ValueError naming the offending format string so the failure message is self-explanatory
    raise ValueError(f"Unsupported source_format: {source_format}")

if df.rdd.isEmpty():
    print(f"No files at {landing_path} — skipping.")
    job.commit() 
    sys.exit(0) # Short circuit the job

# Add ingested_date column to the dataframe
df = df.withColumn("ingested_date", F.lit(ingested_date))

# ── Write to Iceberg ────────────────────────────────────────────────────────────

# Check if the table exists
def table_exists() -> bool:
    """Check the Iceberg/Glue catalog for the target table."""
    try:
        spark.sql(f"DESCRIBE TABLE {ICEBERG_TABLE}")
        return True
    except:
        return False

if not table_exists():
    # If the table doesn't exists create the table and write 
    # the raw data to it using iceberg
    (
        df
        .writeTo(ICEBERG_TABLE)
        # Set partition column
        .partitionedBy("ingested_date")
        # Use Iceberg as the table format
        .using("iceberg")
        # Set Parquet as the storage format for data files
        .tableProperty("write.format.default", "parquet")
        # Snappy compression: high write speed, high read speed, moderate storage costs
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create()
    )
    print(f"[{table}] Created Iceberg table: {ICEBERG_TABLE}")
else:
    # ── Schema evolution ─────────────────────────────────────────────────

    # Read the target Iceberg table as a DataFrame so we can inspect its current schema
    target_df = spark.table(ICEBERG_TABLE)

    # Capture the target table's columns as an ordered list — we'll preserve this order when re-aligning
    target_cols = list(target_df.columns)

    # Capture the incoming dataframe's columns as a set for fast difference operations
    df_cols = set(df.columns)

    # Find columns present in the CSV but not in the target table (sorted for deterministic ordering)
    new_cols = sorted(df_cols - set(target_cols))

    # Find columns present in the target table but missing from the CSV
    missing_cols = sorted(set(target_cols) - df_cols)

    # Loop over each new column so we can evolve the target table to include it
    for col in new_cols:
        # Get the SQL-compatible type string (e.g. "decimal(10,2)") for this column from the dataframe schema
        col_type = df.schema[col].dataType.simpleString()
        # Issue an ALTER TABLE ADD COLUMN against the target table to extend its schema
        spark.sql(f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN {col} {col_type}")
        # Log the addition so unexpected upstream changes are visible in run logs
        print(f"[{table}] Added column {col} ({col_type}) to {ICEBERG_TABLE}")

    # Loop over each column missing from the CSV so we can backfill it on the dataframe
    for col in missing_cols:
        # Look up the column's data type from the target table's schema
        col_type = target_df.schema[col].dataType
        # Add the column to the dataframe as a literal NULL cast to the target type
        df = df.withColumn(col, F.lit(None).cast(col_type))

    # Reorder the dataframe columns to match the evolved table: existing target columns first, then new ones
    df = df.select(*target_cols, *new_cols)

    # Write the dataframe, overwriting only the partition for this ingested_date (idempotent on re-runs)
    df.writeTo(ICEBERG_TABLE).overwritePartitions()
    # Log which partition was overwritten
    print(f"[{table}] Overwrote partition ingested_date={ingested_date}")

count = spark.sql(f"""
    SELECT COUNT(*) FROM {ICEBERG_TABLE}
    WHERE ingested_date = '{ingested_date}'
""").collect()[0][0]
print(f"[{table}] {count:,} rows in partition ingested_date={ingested_date}")

# Commit this glue job
job.commit()