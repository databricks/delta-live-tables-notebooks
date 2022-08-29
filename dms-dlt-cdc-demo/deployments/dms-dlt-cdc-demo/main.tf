provider "databricks" {}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      App        = "DMS DLT CDC Demo"
      Owner       = "Databricks"
    }
  }
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = "${random_string.naming.result}"
}

module "vpc" {
  source = "../../modules/aws-vpc"
}

module "rds" {
  source       = "../../modules/aws-rds-mysql"
  subnets      = module.vpc.private_subnets
  rds_username = var.rds_username
  rds_password = var.rds_password
  rds_port     = var.rds_port
  rds_database = var.rds_database
}

module "lambda-function" {
  source       = "../../modules/aws-lambda-rds"
  subnets      = module.vpc.private_subnets
  security_group_ids = [module.vpc.default_sg]
  rds_hostname = module.rds.rds_hostname
  rds_username = var.rds_username
  rds_password = var.rds_password
  rds_database = var.rds_database
  rds_port     = var.rds_port
}

module "lambda-invoke-create" {
  source = "../../modules/aws-lambda-invoke"
  operation = "create"
  lambda_function_name = module.lambda-function.name
}

module "lambda-invoke-populate" {
  source = "../../modules/aws-lambda-invoke"
  operation = "populate"
  lambda_function_name = module.lambda-function.name
  depends_on = [
    module.lambda-invoke-create
  ]
}

module "dms-replication-instance" {
  source      = "../../modules/aws-dms-replication-instance"
  subnets     = module.vpc.private_subnets
}

module "dms-endpoint" {
  source      = "../../modules/aws-dms-endpoint"
  rds_hostname = module.rds.rds_hostname
  rds_username = var.rds_username
  rds_password = var.rds_password
  rds_database = var.rds_database
  rds_port    = var.rds_port
  prefix      = local.prefix
}

module "dms-migration-task" {
  source      = "../../modules/aws-dms-migration-task"
  dms_replication_instance_arn = module.dms-replication-instance.dms_replication_instance_arn
  dms_endpoint_mysql_arn = module.dms-endpoint.dms_endpoint_mysql_arn
  dms_endpoint_s3_arn = module.dms-endpoint.dms_endpoint_s3_arn
  depends_on = []
}

resource "null_resource" "delay" {
  provisioner "local-exec" {
    command = "sleep 200"
  }
  depends_on = [
    module.dms-migration-task
  ]
}

module "lambda-invoke-modify" {
  source = "../../modules/aws-lambda-invoke"
  operation = "modify"
  lambda_function_name = module.lambda-function.name
  depends_on = [
    null_resource.delay
  ]
}

module "db-instance-profile" {
  source = "../../modules/aws-db-instance-profile"
  s3_bucket_arn = module.dms-endpoint.s3_bucket_arn
  db_crossaccount_role = var.db_crossaccount_role
}

resource "databricks_notebook" "notebook" {
  content_base64 = filebase64("../../resources/dlt/dms-mysql-cdc-demo.py")
  path     = "/Shared/Demo/aws-dms-cdc"
  language = "PYTHON"
}

module "db-dlt-pipeline" {
  source = "../../modules/db-dlt-pipeline"
  notebook_path = databricks_notebook.notebook.path
  instance_profile_arn = module.db-instance-profile.db_instance_profile
  s3_data_path = module.dms-endpoint.s3_bucket_name
  rds_database = var.rds_database
  prefix       = local.prefix
  depends_on = [
    module.dms-migration-task
  ]
}

resource "null_resource" "run_db_dlt_pipeline" {
  provisioner "local-exec" {
    command = "python ../../resources/utils/dlt_runner.py --pipeline-id ${module.db-dlt-pipeline.pipeline_id}"
  }
  depends_on = [
    module.db-dlt-pipeline,
    module.lambda-invoke-modify
  ]
}

resource "databricks_cluster" "analysis" {
  cluster_name            = "dms-mysql-cdc-demo-analysis"
  spark_version           = "10.4.x-scala2.12"
  node_type_id            = "i3.xlarge"
  autotermination_minutes = 15
  aws_attributes {
      instance_profile_arn = module.db-instance-profile.db_instance_profile
    }
  spark_conf = {
    "spark.master" = "local[*, 4]",
    "spark.databricks.cluster.profile" = "singleNode"
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  depends_on = [
    null_resource.run_db_dlt_pipeline
  ]
}

resource "databricks_notebook" "notebook-analysis" {
  content_base64 = filebase64("../../resources/utils/dms-mysql-cdc-analysis.py")
  path     = "/Shared/Demo/aws-dms-cdc-analysis"
  language = "PYTHON"
  depends_on = [
    databricks_cluster.analysis
  ]
}

output "notebook_url" {
  value = databricks_notebook.notebook-analysis.url
  description = "Link to exploratory notebook, go the url and run against cluster"
}