resource "databricks_pipeline" "this" {
  name    = "dms-mysql-cdc"
  target  = "${var.prefix}_dms_mysql_cdc_demo"
  storage = "s3://${var.s3_data_path}/dlt_pipeline"
  photon  = true
  cluster {
    label       = "default"
    num_workers = 1
    custom_tags = {
      cluster_type = "default"
    }
    aws_attributes {
      instance_profile_arn = var.instance_profile_arn
      first_on_demand = 0
    }
  }

  configuration = {
    "data" : "s3://${var.s3_data_path}/data/${var.rds_database}"
  }


  library {
    notebook {
      path = var.notebook_path
    }
  }

  development = true
  channel = "CURRENT"
  edition = "advanced"
  continuous = false
}

output "pipeline_id" {
  value = databricks_pipeline.this.id
}