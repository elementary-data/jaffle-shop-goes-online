import logging
import os
from pathlib import Path
from monitor.dbt_runner import DbtRunner

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
    dbt_runner.run()
    logger.info("Run tests over the validation models")
    dbt_runner.test()

    logger.info("Finish")


if __name__ == '__main__':
    initial_demo()
