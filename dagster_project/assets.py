import os
from typing import List
from dagster import asset, AssetExecutionContext, PipesSubprocessClient, Config, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject, DagsterDbtTranslator
import boto3
from dagster_aws.pipes import PipesS3MessageReader, PipesS3ContextInjector

# 1. Configuration for dbt
DBT_PROJECT_DIR = "/opt/dagster/app/my_dbt_project"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

class ClaimConfig(Config):
    file_keys: List[str] = []

class PolicyConfig(Config):
    file_keys: List[str] = []

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        
        if resource_type == "source":
            # Map the dbt table name to the exact Spark asset function name
            if name == "silver_policy":
                return AssetKey(["policy_processing_spark"])
            if name == "silver_claim":
                return AssetKey(["claim_processing_spark"])
        
        # Keep default behavior for models and other sources
        return super().get_asset_key(dbt_resource_props)

pipes_client = PipesSubprocessClient()
# --- 1. THE SILVER LAYER (Spark) ---

s3_client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minio123",
    aws_secret_access_key="minio123",
    use_ssl=False
)

pipes_client = PipesSubprocessClient(
    # 1. This tells Dagster to put the "Instructions" in S3
    context_injector=PipesS3ContextInjector(
        client=s3_client, 
        bucket="dagster-pipes-metadata"
    ),
    # 2. This tells Dagster to look for "Logs/Metadata" in S3
    message_reader=PipesS3MessageReader(
        client=s3_client, 
        bucket="dagster-pipes-metadata"
    )
)

@asset(compute_kind="PySpark")
def policy_processing_spark(context: AssetExecutionContext, config: PolicyConfig):
    context.log.info(f"Raw config received: {config.file_keys}")
    paths = [f"s3a://raw-data/{k}" for k in config.file_keys]
    env = {"TARGET_FILE": ",".join(paths)}
    # 3. Run the command (without the message_reader argument)
    yield from pipes_client.run(
        context=context,
        env=env,
        command=[
            "docker", "exec", "-i",
            "-e", "DAGSTER_PIPES_CONTEXT",
            "-e", "DAGSTER_PIPES_MESSAGES",
            "-e", "TARGET_FILE",
            "spark-master", 
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "/opt/spark/jobs/policy_processor.py"
        ]
    ).get_results()

@asset(compute_kind="PySpark")
def claim_processing_spark(context: AssetExecutionContext, config: ClaimConfig):
    context.log.info(f"Raw config received: {config.file_keys}")
    paths = [f"s3a://raw-data/{k}" for k in config.file_keys]
    env = {"TARGET_FILE": ",".join(paths)}
    # 3. Run the command (without the message_reader argument) 
    yield from pipes_client.run(
        context=context,
        env=env,
        command=[
            "docker", "exec", "-i",
            "-e", "DAGSTER_PIPES_CONTEXT",
            "-e", "DAGSTER_PIPES_MESSAGES",
            "-e", "TARGET_FILE",
            "spark-master", 
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "/opt/spark/jobs/claim_processor.py"
        ]
    ).get_results()
# --- 2. THE GOLD LAYER (dbt) ---
@dbt_assets(manifest=dbt_project.manifest_path, 
            dagster_dbt_translator=CustomDagsterDbtTranslator(),
            pool="duckdb_write")
def insurance_star_schema(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()