# Import Path so we can build a filesystem path to the Glue script that sits next to this DAG file
from pathlib import Path

# Import the DAG, task, and Param primitives we'll need from the Airflow SDK
#### YOUR CODE HERE

# Import the GlueJob Operator
#### YOUR CODE HERE

# Import the S3Hook
#### YOUR CODE HERE

# ── Config ─────────────────────────────────────────────────────────────────────

# Pull the S3 bucket name from an Airflow Variable via a Jinja template (keeps it out of source control)
S3_BUCKET              = "{{ var.value.s3_bucket }}"
# Top-level prefix in S3 under which the generator script writes landing files
LANDING_PREFIX         = "landing/"
# Build the Iceberg warehouse URI from the bucket — Iceberg writes table data and metadata under here
WAREHOUSE_LOCATION     = f"s3://{S3_BUCKET}/iceberg-warehouse/"
# Resolve the Glue script's path relative to this DAG file and render it as a POSIX-style string
ICEBERG_SCRIPT         = (Path(__file__).parent / "glue_script.py").as_posix()
# Name of the IAM role Glue will assume when it runs the ingestion script
GLUE_ROLE_NAME         = "dev-lakehouse-glue-role"
# Connection id Airflow uses to authenticate with AWS
AWS_CONN_ID            = "aws_default"
# AWS region everything lives in
REGION                 = "us-east-1"


# Initialize a DAG 
# - Set the id to "raw_ingester"
# - Set a manual schedule
# - Define a runtime parameter the behaves like a dropdown
#     - Set the dropdown options to the two data intervals in S3
# - Set maximum active runs to 1
# - Set maximum active tasks to 2
#### YOUR CODE HERE

    # Define a task that lists every table prefix sitting under the chosen data interval in S3
    @task
    def capture_landing_keys(s3_bucket, params):

        # Instantiate an S3Hook against the configured AWS connection
        #### YOUR CODE HERE

        # Compose the S3 prefix for the selected data interval (e.g. "landing/2026-01-01/")
        #### YOUR CODE HERE

        # Collect all table prefixes stored
        # in the data interval and push
        # them to the xcom
        #### YOUR CODE HERE

    # Define a task that, for each table prefix, peeks at the first file's extension to determine the source format
    @task
    def determine_format(s3_bucket, landing_keys, params):

        # Instantiate an S3Hook against the configured AWS connection
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Initialize an empty list to accumulate one params dict per table
        table_params = []

        # Iterate over each table prefix returned from the upstream task
        for prefix in landing_keys:

            # Compose the full S3 prefix for this table within the chosen data interval
            ingestion_prefix = f"{LANDING_PREFIX}{params['data_interval']}/{prefix}/"

            # List every file key under this table's prefix
            keys = hook.list_keys(
                bucket_name=s3_bucket,
                prefix=prefix
                )
            
            # Append a dict carrying this table's prefix and its file format (parsed from the first file's extension)
            table_params.append(
                {'key': prefix, 'format': keys[0].split('.')[-1]}
                )
        
        # Return the list of per-table param dicts so the next operator can fan out across them
        return table_params


    # Call the first task to push the list of table prefixes onto XCom
    table_keys = capture_landing_keys()

    # Call the second task with the bucket and the prefix list to produce the per-table params
    table_params = determine_format(S3_BUCKET, table_keys)

    # Use GlueJobOperator.partial(...).expand_kwargs(...) to fan out one Glue job per table.
    # Static, shared config goes in .partial(); per-table arguments come from .expand_kwargs().
    submit_glue_jobs = GlueJobOperator.partial(
        task_id="ingest",
        aws_conn_id=AWS_CONN_ID,
        script_location=ICEBERG_SCRIPT,
        s3_bucket=S3_BUCKET,
        iam_role_name=GLUE_ROLE_NAME,
        region_name=REGION,
        # Block until the Glue job finishes so downstream tasks see real success/failure
        #### YOUR CODE HERE

        # Re-upload the Glue script to S3 each run so local edits to glue_script.py take effect
        #### YOUR CODE HERE

        # Render each mapped task's UI label from its --table arg so the grid view shows table names
        map_index_template="{{ task.script_args['--table'] }}",
        # Stream Glue job logs back to the Airflow task logs for easier debugging
        #### YOUR CODE HERE

        # Define the underlying Glue job's runtime config: engine version, workers, and Iceberg/Spark options
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                # Tell Glue to bundle Iceberg's runtime jars on the cluster
                "--datalake-formats": "iceberg",
                "--conf": (
                    # Register Iceberg's Spark SQL extensions so Iceberg-specific procedures, DDL, and writer hints work in spark.sql()
                    #### YOUR CODE HERE
                    # Declare a Spark catalog named "iceberg" backed by Iceberg's SparkCatalog —
                    # this is what lets us reference tables as iceberg.<namespace>.<table>
                    #### YOUR CODE HERE
                    # Back the iceberg catalog with the AWS Glue Data Catalog so table metadata is persisted in Glue
                    # and visible to other AWS services (Athena, EMR, etc.)
                    #### YOUR CODE HERE
                    # Use Iceberg's native S3FileIO for data-file I/O — faster and more direct than routing through Hadoop's S3A
                    #### YOUR CODE HERE
                    # Root S3 location where this catalog stores table data files and metadata
                    #### YOUR CODE HERE
                    # Set partition overwrite mode to dynamic so .overwritePartitions() only replaces partitions
                    # touched by the incoming dataframe and leaves the rest of the table alone
                    #### YOUR CODE HERE
                ),
            },
        }
    ).expand_kwargs(
        # For each table_params entry, build a kwargs dict (job_name + script_args) — one mapped task per dict
        table_params.map(lambda table: {
            # Derive a unique job_name per table from the prefix's last path segment
            "job_name": f"ingest_raw_{table['key'].strip('/').split('/')[-1]}",
            # Pass the per-table CLI args that the Glue script reads via getResolvedOptions
            "script_args": {
                "--table": table['key'].strip("/").split("/")[-1],
                "--landing_path": f"s3://{S3_BUCKET}/{table['key']}",
                "--ingested_date": "{{ params['data_interval'] }}",
                "--source_format": table['format']
            }
        })
    )