import boto3
from dagster import sensor, RunRequest, SensorEvaluationContext
import json
@sensor(job_name="claim_job")
def claim_sensor(context: SensorEvaluationContext):
    s3_client = boto3.client("s3", endpoint_url="http://minio:9000", 
                             aws_access_key_id="minio123", aws_secret_access_key="minio123")
    bucket = "raw-data"
    
    # Simple string cursor
    last_ts = context.cursor or "1970-01-01T00:00:00"

    def get_new_files(prefix, current_ts):
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        # Filter out folder markers (Size 0) and older files
        files = [f for f in resp.get("Contents", []) 
                 if f["Size"] > 0 and f["LastModified"].isoformat() > current_ts]
        return sorted(files, key=lambda x: x["LastModified"])

    new_claim = get_new_files("claim/", last_ts)

    if new_claim:
        file_keys = [f["Key"] for f in new_claim]
        yield RunRequest(
            run_key=f"claims_batch_{new_claim[-1]['LastModified'].isoformat()}",
            run_config={
                "ops": {
                    "claim_processing_spark": {
                        "config": {"file_keys": file_keys}
                    }
                }
            }
        )
        # Update with plain string
        context.update_cursor(new_claim[-1]["LastModified"].isoformat())

@sensor(job_name="policy_job")
def policy_sensor(context: SensorEvaluationContext):
    s3_client = boto3.client("s3", endpoint_url="http://minio:9000", 
                             aws_access_key_id="minio123", aws_secret_access_key="minio123")
    bucket = "raw-data"
    
    # REMOVED JSON.LOADS - Use plain string
    last_ts = context.cursor or "1970-01-01T00:00:00"

    def get_new_files(prefix, current_ts):
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [f for f in resp.get("Contents", []) 
                 if f["Size"] > 0 and f["LastModified"].isoformat() > current_ts]
        return sorted(files, key=lambda x: x["LastModified"])
    
    new_policy = get_new_files("policy/", last_ts)

    if new_policy:
        file_keys = [f["Key"] for f in new_policy]
        yield RunRequest(
            run_key=f"policies_batch_{new_policy[-1]['LastModified'].isoformat()}",
            run_config={
                "ops": {
                    "policy_processing_spark": {
                        "config": {"file_keys": file_keys}
                    }
                }
            }
        )
        # Update with plain string
        context.update_cursor(new_policy[-1]["LastModified"].isoformat())