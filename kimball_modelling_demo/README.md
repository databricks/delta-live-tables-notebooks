# kimball_modelling_demo

This project shows how you can use Delta Live Tables and Databricks Asset Bundles to build a data warehouse using Kimball modelling techniques.

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy -p [my_profile]
    ```

4. To run the main job, use the "run" command:
   ```
   $ databricks bundle run kimball_modelling_demo -p [my_profile]
   ```

For documentation on the Databricks asset bundles format used for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
