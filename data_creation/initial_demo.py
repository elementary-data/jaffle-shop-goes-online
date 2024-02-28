import argparse
import logging
from typing import Optional
from data_creation.data_injection.inject_jaffle_shop_exposures import (
    inject_jaffle_shop_exposures,
)

from data_creation.data_injection.inject_jaffle_shop_tests import (
    inject_jaffle_shop_tests,
)
from data_creation.incremental_data_creation.incremental_data_flow import (
    run_incremental_data_creation,
)

logger = logging.getLogger(__name__)


def initial_incremental_demo(
    target: Optional[str] = None, days_back=30, profiles_dir: Optional[str] = None
):
    run_incremental_data_creation(
        target=target, profiles_dir=profiles_dir, days_back=days_back
    )
    inject_jaffle_shop_tests(target=target, profiles_dir=profiles_dir)
    inject_jaffle_shop_exposures(target=target, profiles_dir=profiles_dir)


def main():
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-t", "--target", required=True)
    args_parser.add_argument("-d", "--days-back", type=int, default=8)
    args_parser.add_argument("-pd", "--profiles-dir")
    args = args_parser.parse_args()

    initial_incremental_demo(
        target=args.target, days_back=args.days_back, profiles_dir=args.profiles_dir
    )


if __name__ == "__main__":
    main()
