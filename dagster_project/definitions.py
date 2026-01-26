import os
import sys

# Add the current directory to sys.path so 'import assets' works
sys.path.append(os.path.dirname(__file__))

from dagster import (
    AssetSelection, 
    Definitions, 
    load_assets_from_modules, 
    define_asset_job, 
    PipesSubprocessClient
)
from dagster_dbt import DbtCliResource, DbtProject
import assets
import sensors

# 1. Path to your dbt project
DBT_PROJECT_DIR = "/opt/dagster/app/my_dbt_project"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

# 2. Define Targeted Jobs
# These selections ensure that when a sensor fires, ONLY the specific asset runs.
# We use .required_multi_asset_neighbors() to ensure dbt doesn't break if it shares a module.

claim_job = define_asset_job(
    name="claim_job",
    selection=(
        AssetSelection.keys("claim_processing_spark") # The Spark Asset
        | AssetSelection.keys("claim_processing_spark").downstream() # Downstream dbt models
    )
)

# Job for Policies: Run Spark + only the dbt models affected by policies
policy_job = define_asset_job(
    name="policy_job",
    selection=(
        AssetSelection.keys("policy_processing_spark") # The Spark Asset
        | AssetSelection.keys("policy_processing_spark").downstream() # Downstream dbt models
    )
)

# 3. Resources
resources = {
    "pipes_client": PipesSubprocessClient(),
    "dbt": DbtCliResource(project_dir=dbt_project),
}

# 4. Load everything from assets.py
all_assets = load_assets_from_modules([assets])

# 5. Final Export
defs = Definitions(
    assets=all_assets,
    sensors=[sensors.claim_sensor, sensors.policy_sensor],
    jobs=[claim_job, policy_job], # Add targeted jobs here
    resources=resources
)