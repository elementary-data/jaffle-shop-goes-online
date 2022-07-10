import logging
import os
from pathlib import Path
from monitor.dbt_runner import DbtRunner
from datetime import datetime, timedelta
import glob
import random

from data_creation.incremental_training_data_generator import generate_incremental_training_data
from data_creation.incremental_validation_data_generator import generate_incremental_validation_data
from data_creation.jaffle_shop_utils.csv import clear_csv

logger = logging.getLogger(__name__)

JAFFLE_SHOP_ONLINE_DIR_NAME = "jaffle_shop_online"
REPO_DIR = Path(os.path.dirname(__file__)).parent.absolute()
DBT_PROJECT_DIR = os.path.join(REPO_DIR, JAFFLE_SHOP_ONLINE_DIR_NAME)
DBT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')


def initial_demo(target = None):
    dbt_runner = DbtRunner(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, target=target)
    
    logger.info("Clear demo environment")
    dbt_runner.run_operation(macro_name='clear_tests')

    logger.info("Initial training data")
    logger.info("Seed training data")
    dbt_runner.seed(select="training")
    logger.info("Creates training models")
    dbt_runner.run()
    logger.info("Run tests over the training models")
    dbt_runner.test()

    logger.info("Initial validation data")
    logger.info("Seed validation data")
    dbt_runner.seed(select="validation")
    logger.info("Creates validation models")
    dbt_runner.run(vars={"validation": True})
    logger.info("Run tests over the validation models")
    dbt_runner.test()

    logger.info("Finish")


def initial_incremental_demo(target = None, days_back = 30):
    dbt_runner = DbtRunner(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, target=target)
    first_run = True
    
    logger.info("Clear demo environment")
    dbt_runner.run_operation(macro_name='clear_tests')
    clear_data(validation=True, training=True)

    logger.info(f"Running incremental demo fo {days_back} days back")
    current_time = datetime.now()
    for run_index in range(1, days_back):
        custom_run_time = current_time - timedelta(days_back - run_index)

        if not first_run and not random.randint(0, round(days_back / 4)):
            clear_data(validation=True)
            generate_incremental_validation_data(custom_run_time)
            dbt_runner.seed(select='validation')
            dbt_runner.run(vars={"custom_run_started_at": custom_run_time.isoformat(), "validation": True})
            dbt_runner.test(vars={"custom_run_started_at": custom_run_time.isoformat(), "validation": True})
            clear_data(validation=True)
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select='training')
            dbt_runner.run(vars={"custom_run_started_at": custom_run_time.isoformat()})

        else:    
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select='training')
            dbt_runner.run(vars={"custom_run_started_at": custom_run_time.isoformat()})
            dbt_runner.test(vars={"custom_run_started_at": custom_run_time.isoformat()})

        first_run = False
    
    clear_data(validation=True)
    generate_incremental_validation_data(current_time, ammount_of_new_data=250)
    dbt_runner.seed(select='validation')
    dbt_runner.run(vars={"custom_run_started_at": current_time.isoformat(), "validation": True})
    dbt_runner.test(vars={"custom_run_started_at": current_time.isoformat(), "validation": True})


def clear_data(validation = False, training = False):
    CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
    NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH = "../jaffle_shop_online/seeds/training"
    NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH = "../jaffle_shop_online/seeds/validation"

    training_path = os.path.join(CURRENT_DIRECTORY_PATH, NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH)
    validation_path = os.path.join(CURRENT_DIRECTORY_PATH, NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH)

    if validation:
        for csv_file in glob.glob(validation_path + "/*.csv"):
            clear_csv(csv_file)

    if training:
        for csv_file in glob.glob(training_path + "/*.csv"):
            clear_csv(csv_file)


if __name__ == '__main__':
    initial_demo()
