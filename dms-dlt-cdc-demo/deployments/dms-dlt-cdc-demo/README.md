<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | ~> 4.26.0 |
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~> 1.2.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_databricks"></a> [databricks](#provider\_databricks) | 1.2.1 |
| <a name="provider_null"></a> [null](#provider\_null) | 3.1.1 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.3.2 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_db-dlt-pipeline"></a> [db-dlt-pipeline](#module\_db-dlt-pipeline) | ../../modules/db-dlt-pipeline | n/a |
| <a name="module_db-instance-profile"></a> [db-instance-profile](#module\_db-instance-profile) | ../../modules/aws-db-instance-profile | n/a |
| <a name="module_dms-endpoint"></a> [dms-endpoint](#module\_dms-endpoint) | ../../modules/aws-dms-endpoint | n/a |
| <a name="module_dms-migration-task"></a> [dms-migration-task](#module\_dms-migration-task) | ../../modules/aws-dms-migration-task | n/a |
| <a name="module_dms-replication-instance"></a> [dms-replication-instance](#module\_dms-replication-instance) | ../../modules/aws-dms-replication-instance | n/a |
| <a name="module_lambda-function"></a> [lambda-function](#module\_lambda-function) | ../../modules/aws-lambda-rds | n/a |
| <a name="module_lambda-invoke-create"></a> [lambda-invoke-create](#module\_lambda-invoke-create) | ../../modules/aws-lambda-invoke | n/a |
| <a name="module_lambda-invoke-modify"></a> [lambda-invoke-modify](#module\_lambda-invoke-modify) | ../../modules/aws-lambda-invoke | n/a |
| <a name="module_lambda-invoke-populate"></a> [lambda-invoke-populate](#module\_lambda-invoke-populate) | ../../modules/aws-lambda-invoke | n/a |
| <a name="module_rds"></a> [rds](#module\_rds) | ../../modules/aws-rds-mysql | n/a |
| <a name="module_vpc"></a> [vpc](#module\_vpc) | ../../modules/aws-vpc | n/a |

## Resources

| Name | Type |
|------|------|
| [databricks_cluster.analysis](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/cluster) | resource |
| [databricks_notebook.notebook](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notebook) | resource |
| [databricks_notebook.notebook-analysis](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notebook) | resource |
| [null_resource.delay](https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource) | resource |
| [null_resource.run_db_dlt_pipeline](https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource) | resource |
| [random_string.naming](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_db_crossaccount_role"></a> [db\_crossaccount\_role](#input\_db\_crossaccount\_role) | name of cross account role used for Databricks | `string` | n/a | yes |
| <a name="input_rds_database"></a> [rds\_database](#input\_rds\_database) | Database to create tables in | `string` | `"databricks"` | no |
| <a name="input_rds_password"></a> [rds\_password](#input\_rds\_password) | password to use for the root rds password, rds spun up is in private subnet | `string` | `"databricks1"` | no |
| <a name="input_rds_port"></a> [rds\_port](#input\_rds\_port) | Database port | `number` | `3306` | no |
| <a name="input_rds_username"></a> [rds\_username](#input\_rds\_username) | username to use for the root rds user | `string` | `"databricks_root"` | no |
| <a name="input_region"></a> [region](#input\_region) | AWS Region | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | Default Tags to identify resource | `list(string)` | <pre>[<br>  "Databricks",<br>  "DMS CDC DLT DEMO"<br>]</pre> | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_notebook_url"></a> [notebook\_url](#output\_notebook\_url) | Link to exploratory notebook, go the url and run against cluster |
<!-- END_TF_DOCS -->