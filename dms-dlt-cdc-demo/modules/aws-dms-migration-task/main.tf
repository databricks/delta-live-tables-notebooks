# Create a new replication task
resource "aws_dms_replication_task" "this" {
  migration_type            = "full-load-and-cdc"
  replication_instance_arn  = var.dms_replication_instance_arn
  replication_task_id       = "mysql-s3-dms-replication-task-tf"
  source_endpoint_arn       = var.dms_endpoint_mysql_arn
  table_mappings            = "{\"rules\":[{\"rule-type\":\"selection\",\"rule-id\":\"1\",\"rule-name\":\"1\",\"object-locator\":{\"schema-name\":\"%\",\"table-name\":\"%\"},\"rule-action\":\"include\"}]}"
  start_replication_task    = true
  target_endpoint_arn = var.dms_endpoint_s3_arn

  lifecycle {
	  ignore_changes = [replication_task_settings]
  }
}