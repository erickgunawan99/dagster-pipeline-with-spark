from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession
from dagster_pipes import open_dagster_pipes, PipesS3MessageWriter, PipesS3ContextLoader
import boto3
from pyspark.sql import functions as F
import os


# 1. Define the deepest level first: Beneficiary
beneficiary_schema = StructType([
    StructField("name", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# 2. Define the middle level: Policy Holder (contains the beneficiary struct)
holder_schema = StructType([
    StructField("name", StringType(), True),
    StructField("dob", StringType(), True), # ISO format dates are strings in JSON
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("income", DoubleType(), True),
    StructField("occupation", StringType(), True),
    StructField("marriage_status", StringType(), True),
    StructField("children", IntegerType(), True),
    StructField("beneficiary", beneficiary_schema, True) # The nested struct
])

# 3. Define the Asset struct (for the array)
asset_schema = StructType([
    StructField("asset_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("value", DoubleType(), True)
])

# 4. The Final Root Schema
policy_schema = StructType([
    StructField("policy_id", StringType(), True),
    StructField("policy_date", StringType(), True),
    StructField("tenure_years", IntegerType(), True),
    StructField("premium_amount", DoubleType(), True),
    StructField("client_id", StringType(), True),
    StructField("asset", asset_schema, True), # The array of assets
    StructField("policy_holder", holder_schema, True)   # The nested holder
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
            path_env = os.getenv("TARGET_FILE")
            path_list = path_env.split(",")

            if not path_list:
                pipes.log.error("CRITICAL: TARGET_FILE environment variable is not set!")
                # Raising an error here will cause the Dagster Asset to fail
                raise ValueError("Missing required environment variable: TARGET_FILE")
            
              # 2. Log the specific path/keys we are about to process
            pipes.log.info(f"Processing target paths: {path_list}")

            pipes.log.info("Spark Session Started. Starting Policy raw processing...")
            policy_df = spark.read.schema(policy_schema). \
                option("multiLine", "true"). \
                json(path_list[0] if len(path_list) == 1 else path_list)
            # 1. Clean and Lowercase Policy Data
            parse_policy_df = policy_df.select(
                # Basic IDs
                F.lower(F.col("policy_id")).alias("policy_id"),
                F.to_date(F.col("policy_date")).alias("policy_date"),
                F.col("tenure_years").alias("tenure_years"),
                F.col("premium_amount").alias("premium_amount"),
                F.lower(F.col("client_id")).alias("client_id"),

                # Asset Details
                F.lower(F.col("asset.asset_id")).alias("asset_id"),
                F.lower(F.col("asset.type")).alias("asset_type"),
                F.col("asset.value").alias("asset_value"), # Keeping numeric as-is

                # Policy Holder Details
                F.lower(F.col("policy_holder.name")).alias("policy_holder_name"),
                F.to_date(F.col("policy_holder.dob")).alias("policy_holder_dob"),
                F.lower(F.col("policy_holder.city")).alias("policy_holder_city"),
                F.lower(F.col("policy_holder.state")).alias("policy_holder_state"),
                F.col("policy_holder.income").alias("policy_holder_income"),
                F.lower(F.col("policy_holder.occupation")).alias("policy_holder_occupation"),
                F.lower(F.col("policy_holder.marriage_status")).alias("policy_holder_marriage_status"),
                F.col("policy_holder.children").alias("policy_holder_children"),

                # Beneficiary Details with Enterprise Standard N/A
                F.coalesce(
                    F.lower(F.col("policy_holder.beneficiary.name")), 
                    F.lit("n/a")
                ).alias("policy_holder_beneficiary_name"),

                F.coalesce(
                    F.lower(F.col("policy_holder.beneficiary.relationship")), 
                    F.lit("n/a")
                ).alias("policy_holder_beneficiary_relationship"),

                F.coalesce(
                    F.lower(F.col("policy_holder.beneficiary.city")), 
                    F.lit("n/a")
                ).alias("policy_holder_beneficiary_city"),
                F.coalesce(
                    F.lower(F.col("policy_holder.beneficiary.state")), 
                    F.lit("n/a")
                ).alias("policy_holder_beneficiary_state")
            )

            # 5. Write the final OBT to MinIO Silver Layer
            parse_policy_df.write \
                .mode("append") \
                .partitionBy("asset_type") \
                .parquet("s3a://silver/policy/")

            row_count = parse_policy_df.count()
            pipes.report_asset_materialization(
                metadata={
                    "row_count": row_count, # Example: pass actual df.count()
                    "status": "Silver Policies Written to MinIO",
                    "storage_path": "s3a://silver/policy/"
                }
            )
    except Exception as e:
        print(f"Error during Pipes operation: {e}")
        raise e

if __name__ == "__main__":
    main()