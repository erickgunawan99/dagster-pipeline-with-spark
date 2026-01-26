import json
import uuid
import random
import io
from datetime import datetime
import boto3
from faker import Faker

fake = Faker()
s3_client = boto3.client("s3", endpoint_url="http://localhost:9000", 
                         aws_access_key_id="minio123", aws_secret_access_key="minio123")

def upload_to_minio(data, bucket, folder, filename):
    json_data = json.dumps(data, indent=2)
    s3_client.upload_fileobj(io.BytesIO(json_data.encode("utf-8")), bucket, f"{folder}/{filename}")
    print(f"Uploaded: s3://{bucket}/{folder}/{filename}")

def generate_normalized_data(num_records=500):
    policies = []
    claims = []

    for _ in range(num_records):
        client_id = f"CL-{uuid.uuid4().hex[:6].upper()}"
        policy_id = f"POL-{uuid.uuid4().hex[:8].upper()}"
        
        marriage_status = random.choice(["Single", "Married", "Divorced"])
        num_children = 0 if marriage_status == "Single" else random.randint(1, 4)
        
        # Generate Assets first to check for "Life" insurance
        asset = {
                "asset_id": f"{policy_id}-ASST-1",
                "type": random.choice(["Auto", "Home", "Life"]),
                "value": random.randint(10000000, 1000000000)
            }


        has_life_insurance = asset.get('type').lower() == 'life'

        # 1. POLICY DATA (Nested Holder and Beneficiary)
        policy_entry = {
            "policy_id": policy_id,
            "policy_date": datetime.now().isoformat(),
            "tenure_years": random.randint(1, 30),
            "premium_amount": random.randint(50000, 2000000),
            "client_id": client_id,
            "asset": asset,
            "policy_holder": {
                "name": fake.name(),
                "dob": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
                "city": fake.city(),
                "state": fake.state(),
                "income": random.randint(30000, 40000000),
                "occupation": random.choice(["Engineer", "Teacher", "Doctor", "Artist"]),
                "marriage_status": marriage_status,
                "children": num_children,
                # Beneficiary logic: Required for Life, optional/null otherwise
                "beneficiary": {
                    "name": fake.name(),
                    "relationship": random.choice(["Spouse", "Child", "Parent", "Sibling", "Friend", "Other"] if marriage_status != "Single" and num_children > 0
                                                   else ["Parent", "Sibling", "Friend", "Other"]),
                    "city": fake.city(),
                    "state": fake.state()
                } if has_life_insurance else None
            }
        }
        policies.append(policy_entry)

        # 2. CLAIMS DATA (Joined by policy_id)
        if random.random() < 0.7:  # 70% chance of having a claim
            claims.append({
                "claim_id": f"CLM-{uuid.uuid4().hex[:6].upper()}",
                "policy_id": policy_id,
                "payout": random.randint(1000000, 50000000),
                "event_date": datetime.now().isoformat()
            })

    return policies, claims

if __name__ == "__main__":
    p_data, clm_data = generate_normalized_data(5000)
    upload_to_minio(p_data, "raw-data", "policy", f"policy_{datetime.now().strftime('%H%M%S')}.json")
    upload_to_minio(clm_data, "raw-data", "claim", f"claim_{datetime.now().strftime('%H%M%S')}.json")
    print("Sample Policy Data:", json.dumps(p_data[0], indent=2))
    print("Sample Claim Data:", json.dumps(clm_data[0], indent=2))