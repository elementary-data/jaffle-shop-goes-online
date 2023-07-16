# jaffle-shop-goes-online

Elementary UI demo dbt project!

## Generate new demo

To generate a new demo please do the following steps:

1. Change `jaffle_shop_online/packages.yml` elementary package to point to your local package - for example:
   ```
   packages:
    - local: /Users/idanshavit/workspace/dbt-data-reliability
    - package: dbt-labs/dbt_utils
      version: 1.0.0
   ```
2. Fetch all branches on your local elementary dbt package
3. Checkout to the branch `now-uses-custom-run-started-at` (merge `master` into it to make sure it is updated)
4. On the `jaffle-shop-goes-online` project run `python data_creation/initial_demo.py`
5. Fetch all branches on your local elementary python package
6. Checkout to the branch `oss-demo-creation` (merge `master` into it to make sure it is updated)
7. Generate the report using `edr report -d 14 --env <env name like Snowflake prod>` command   
