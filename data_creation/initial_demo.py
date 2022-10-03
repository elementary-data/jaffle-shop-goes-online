import glob
import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from data_creation.incremental_training_data_generator import generate_incremental_training_data
from data_creation.incremental_validation_data_generator import generate_incremental_validation_data
from data_creation.jaffle_shop_utils.csv import clear_csv

from elementary.clients.dbt.dbt_runner import DbtRunner

logger = logging.getLogger(__name__)

JAFFLE_SHOP_ONLINE_DIR_NAME = 'jaffle_shop_online'
REPO_DIR = Path(os.path.dirname(__file__)).parent.absolute()
DBT_PROJECT_DIR = os.path.join(REPO_DIR, JAFFLE_SHOP_ONLINE_DIR_NAME)
DBT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')


def initial_demo(target=None):
    dbt_runner = DbtRunner(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, target=target,
                           raise_on_failure=False)

    logger.info('Clear demo environment')
    dbt_runner.run_operation(macro_name='clear_tests')

    logger.info('Seeding training data')
    dbt_runner.seed(select='training')
    logger.info('Running training models')
    dbt_runner.run()
    logger.info('Running tests over the training models')
    dbt_runner.test()

    logger.info('Seeding validation data')
    dbt_runner.seed(select='validation')
    logger.info('Running validation models')
    dbt_runner.run(vars={'validation': True})
    logger.info('Running tests over the validation models')
    dbt_runner.test()


def initial_incremental_demo(target=None, days_back=30):
    dbt_runner = DbtRunner(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR, target=target,
                           raise_on_failure=False)
    first_run = True

    logger.info('Clearing demo environment')
    dbt_runner.run_operation(macro_name='clear_tests')
    clear_data(validation=True, training=True)

    logger.info(f'Running incremental demo for {days_back} days back')
    current_time = datetime.utcnow()
    for run_index in range(1, days_back):
        print(f'Running the [{run_index}/{days_back}] day.')
        custom_run_time = current_time - timedelta(days_back - run_index)

        if not first_run and not random.randint(0, round(days_back / 4)):
            clear_data(validation=True)
            generate_incremental_validation_data(custom_run_time)
            dbt_runner.seed(select='validation')
            dbt_runner.run(vars={'custom_run_started_at': custom_run_time.isoformat(), 'validation': True})
            dbt_runner.test(vars={'custom_run_started_at': custom_run_time.isoformat(), 'validation': True})
            clear_data(validation=True)
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select='training')
            dbt_runner.run(vars={'custom_run_started_at': custom_run_time.isoformat()})

        else:
            generate_incremental_training_data(custom_run_time)
            dbt_runner.seed(select='training')
            dbt_runner.run(vars={'custom_run_started_at': custom_run_time.isoformat()})
            dbt_runner.test(vars={'custom_run_started_at': custom_run_time.isoformat()})

        first_run = False

    clear_data(validation=True)
    generate_incremental_validation_data(current_time, ammount_of_new_data=600)
    dbt_runner.seed(select='validation')
    dbt_runner.run(vars={'custom_run_started_at': current_time.isoformat(), 'validation': True})
    dbt_runner.test(vars={'custom_run_started_at': current_time.isoformat(), 'validation': True})


def clear_data(validation=False, training=False):
    current_directory_path = os.path.dirname(os.path.realpath(__file__))
    new_jaffle_training_data_direcorty_relative_path = '../jaffle_shop_online/seeds/training'
    new_jaffle_validation_data_direcorty_relative_path = '../jaffle_shop_online/seeds/validation'

    training_path = os.path.join(current_directory_path, new_jaffle_training_data_direcorty_relative_path)
    validation_path = os.path.join(current_directory_path, new_jaffle_validation_data_direcorty_relative_path)

    if validation:
        for csv_file in glob.glob(validation_path + '/*.csv'):
            clear_csv(csv_file)

    if training:
        for csv_file in glob.glob(training_path + '/*.csv'):
            clear_csv(csv_file)


if __name__ == '__main__':
    # initial_demo()
    initial_incremental_demo()
