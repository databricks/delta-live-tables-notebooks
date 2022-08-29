<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | 1.0.0 |

## Providers

No providers.

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_this"></a> [this](#module\_this) | ../../modules/aws-e2workspace | n/a |

## Resources

No resources.

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_databricks_account_id"></a> [databricks\_account\_id](#input\_databricks\_account\_id) | Databricks Account ID | `any` | n/a | yes |
| <a name="input_databricks_account_password"></a> [databricks\_account\_password](#input\_databricks\_account\_password) | Databricks User Password | `any` | n/a | yes |
| <a name="input_databricks_account_username"></a> [databricks\_account\_username](#input\_databricks\_account\_username) | Databricks Username (Needs to be Account Console Admin) | `any` | n/a | yes |
| <a name="input_region"></a> [region](#input\_region) | AWS Region to Deploy Databricks In | `any` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_cross_account_name"></a> [cross\_account\_name](#output\_cross\_account\_name) | Cross Account Name to use for dms-dlt-cdc-demo tf deployment |
| <a name="output_databricks_host"></a> [databricks\_host](#output\_databricks\_host) | Databricks Workspace URL, used for dms-dltc-cdc tf deployment |
<!-- END_TF_DOCS -->