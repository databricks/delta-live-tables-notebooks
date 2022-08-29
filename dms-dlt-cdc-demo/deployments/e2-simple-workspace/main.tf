terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.0.0"
    }
  }
}

variable "databricks_account_id" {
  description = "Databricks Account ID"
}
variable "databricks_account_username" {
  description = "Databricks Username (Needs to be Account Console Admin)"
}
variable "databricks_account_password" {
  description = "Databricks User Password"
}
variable "region" {
  description = "AWS Region to Deploy Databricks In"
}

module "this" {
  source                      = "../../modules/aws-e2workspace"
  databricks_account_id       = var.databricks_account_id
  databricks_account_password = var.databricks_account_password
  databricks_account_username = var.databricks_account_username

  region = var.region

  tags = {
    Owner = var.databricks_account_username
  }
}

provider "databricks" {
  alias = "created_workspace"

  host = module.this.databricks_host
  username = var.databricks_account_username
  password = var.databricks_account_password
  auth_type = "basic"
}

output "databricks_host" {
  value = module.this.databricks_host
  description = "Databricks Workspace URL, used for dms-dltc-cdc tf deployment"
}

output "cross_account_name" {
  value = module.this.cross_account_name
  description = "Cross Account Name to use for dms-dlt-cdc-demo tf deployment"
}