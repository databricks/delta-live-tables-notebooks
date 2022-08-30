terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.26.0"
    }
  }
}