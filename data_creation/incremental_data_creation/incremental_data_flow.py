from datetime import datetime, timedelta
import glob
import logging
import os
from pathlib import Path
import random
from typing import Optional
import uuid

from elementary.clients.dbt.api_dbt_runner import APIDbtRunner
from data_creation.incremental_data_creation.incremental_training_data_generator import (
    generate_incremental_training_data,
)

from data_creation.incremental_data_creation.incremental_validation_data_generator import (
    generate_incremental_validation_data,
)
from utils.csv import clear_csv

logger = logging.getLogger(__name__)

JAFFLE_SHOP_ONLINE_DIR_NAME = "jaffle_shop_online"
REPO_DIR = Path(os.path.dirname(__file__)).parent.parent.absolute()
DBT_PROJECT_DIR = os.path.join(REPO_DIR, JAFFLE_SHOP_ONLINE_DIR_NAME)


def run_incremental_data_creation(
    target: Optional[str] = None, profiles_dir: Optional[str] = None, days_back=30
):
    dbt_runner = APIDbtRunner(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=profiles_dir,
        target=target,
        raise_on_failure=False,
    )

    first_run = True

    logger.info("Clearing demo environment")
    dbt_runner.run_operation(macro_name="jaffle_shop_online.clear_tests")
    clear_data(validation=True, training=True)

    dbt_runner.seed(select="ads")
    dbt_runner.seed(select="sessions")

    logger.info(f"Running incremental demo for {days_back} days back")
    current_time = datetime.utcnow()
    for run_index in range(1, days_back):
        print(f"Running the [{run_index}/{days_back}] day.")
        custom_run_time = current_time - timedelta(days_back - run_index)

        if not first_run and not random.randint(0, round(days_back / 4)):
            clear_data(validation=True)
            generate_incremental_validation_data(custom_run_time)
            dbt_runner.seed(select="validation")
            dbt_runner.run(
                vars={
                    "custom_run_started_at": custom_run_time.isoformat(),
                    "validation": True,
                    "orchestrator": "dbt_cloud",
                    "job_name": "jaffle_shop_online_data_load",
                    "job_id": str(uuid.uuid4()),
                }
            )
            dbt_runner.test(
                vars={
                    "custom_run_started_at": custom_run_time.isoformat(),
                    "validation": True,
                    "orchestrator": "dbt_cloud",
                    "job_name": "jaffle_shop_online_data_test",
                    "job_id": str(uuid.uuid4()),
                }
            )
            clear_data(validation=True)
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select="training")
            dbt_runner.run(
                vars={
                    "custom_run_started_at": custom_run_time.isoformat(),
                    "orchestrator": "dbt_cloud",
                    "job_name": "jaffle_shop_online_data_load",
                    "job_id": str(uuid.uuid4()),
                }
            )

        else:
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select="training")
            dbt_runner.run(
                vars={
                    "custom_run_started_at": custom_run_time.isoformat(),
                    "orchestrator": "dbt_cloud",
                    "job_name": "jaffle_shop_online_data_load",
                    "job_id": str(uuid.uuid4()),
                }
            )
            dbt_runner.test(
                vars={
                    "custom_run_started_at": custom_run_time.isoformat(),
                    "orchestrator": "dbt_cloud",
                    "job_name": "jaffle_shop_online_data_test",
                    "job_id": str(uuid.uuid4()),
                }
            )

        first_run = False

    clear_data(validation=True)
    generate_incremental_validation_data(
        current_time, ammount_of_new_data=600, last_run=True
    )
    dbt_runner.seed(select="validation")
    dbt_runner.run(
        vars={
            "custom_run_started_at": current_time.isoformat(),
            "validation": True,
            "orchestrator": "dbt_cloud",
            "job_name": "jaffle_shop_online_data_load",
            "job_id": str(uuid.uuid4()),
        }
    )
    dbt_runner.test(
        vars={
            "custom_run_started_at": current_time.isoformat(),
            "validation": True,
            "orchestrator": "dbt_cloud",
            "job_name": "jaffle_shop_online_data_test",
            "job_id": str(uuid.uuid4()),
        }
    )


def clear_data(validation=False, training=False):
    current_directory_path = os.path.dirname(os.path.realpath(__file__))
    new_jaffle_training_data_direcorty_relative_path = (
        "../../jaffle_shop_online/seeds/training"
    )
    new_jaffle_validation_data_direcorty_relative_path = (
        "../../jaffle_shop_online/seeds/validation"
    )

    training_path = os.path.join(
        current_directory_path, new_jaffle_training_data_direcorty_relative_path
    )
    validation_path = os.path.join(
        current_directory_path, new_jaffle_validation_data_direcorty_relative_path
    )

    if validation:
        for csv_file in glob.glob(validation_path + "/*.csv"):
            clear_csv(csv_file)

    if training:
        for csv_file in glob.glob(training_path + "/*.csv"):
            clear_csv(csv_file)
