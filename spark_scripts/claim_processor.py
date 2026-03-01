from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from dagster_pipes import open_dagster_pipes, PipesS3MessageWriter, PipesS3ContextLoader
import boto3
from pyspark.sql import functions as F
import os

claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("payout", DoubleType(), True),
    StructField("event_date", StringType(), True)
])
def main():
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio123"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        use_ssl=False
    )
    
    # Define the writer for S3
    message_writer = PipesS3MessageWriter(client=s3_client)
    context_loader = PipesS3ContextLoader(client=s3_client)
    # 1. Initialize the Pipes connection
    # This captures the 'context' sent by Dagster via 'docker exec'
    try:
        with open_dagster_pipes(message_writer=message_writer, context_loader=context_loader) as pipes:
            pipes.log.info("Handshake SUCCESSFUL via S3!")
            # 2. Standard Spark Session setup
            spark = SparkSession.builder \
                .appName("AdTech_Silver_Layer") \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
                .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.access.key", "minio123") \
                .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.cores.max", "1") \
                .config("spark.executor.cores", "1") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077")) \
                .getOrCreate()
            # 1. Force the read from the environment variable
            # If TARGET_FILE isn't set, this will return None
            path_env = os.getenv("TARGET_FILE")
            path_list = path_env.split(",")

            if not path_list:
                pipes.log.error("CRITICAL: TARGET_FILE environment variable is not set!")
                # Raising an error here will cause the Dagster Asset to fail
                raise ValueError("Missing required environment variable: TARGET_FILE")

            # 2. Log the specific path/keys we are about to process
            pipes.log.info(f"Processing target paths: {path_list}")

            pipes.log.info("Spark Session Started. Starting Claims raw processing...")
            claims_df = spark.read.schema(claims_schema). \
                option("multiLine", "true"). \
                json(path_list[0] if len(path_list) == 1 else path_list)


            # 2. Clean and Lowercase Claims
            clean_claims_df = claims_df.select(
                F.lower(F.col("claim_id")).alias("claim_id"),
                F.lower(F.col("policy_id")).alias("claim_policy_id"), # Rename to avoid collision during join
                F.col("payout"),
                F.to_date(F.col("event_date")).alias("claim_event_date")
            )
            # 5. Write to parquet to MinIO bucket
            clean_claims_df.write \
                .mode("append") \
                .partitionBy("claim_event_date") \
                .parquet("s3a://silver/claim/")

            row_count = clean_claims_df.count()
            pipes.report_asset_materialization(
                metadata={
                    "row_count": row_count, # Example: pass actual df.count()
                    "status": "Silver Claims Written to MinIO",
                    "storage_path": "s3a://silver/claim/"
                }
            )
    except Exception as e:
        print(f"Error during Pipes operation: {e}")
        raise e

if __name__ == "__main__":
    main()