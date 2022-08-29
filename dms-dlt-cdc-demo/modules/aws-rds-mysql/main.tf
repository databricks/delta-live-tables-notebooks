resource "aws_db_subnet_group" "this" {
  name       = "rds-sg-grp-dms-db-demo"
  subnet_ids = var.subnets
}

resource "aws_db_parameter_group" "this" {
  name   = "rds-mysql-pg-dms-db-demo"
  family = "mysql5.7"

  parameter {
    name  = "binlog_format"
    value = "ROW"
  }

  parameter {
    name  = "binlog_row_image"
    value = "FULL"
  }
}

resource "aws_db_instance" "this" {
  allocated_storage    = 10
  engine               = "mysql"
  engine_version       = "5.7"
  instance_class       = "db.t3.micro"
  db_name              = var.rds_database
  username             = var.rds_username
  password             = var.rds_password
  backup_retention_period = 10
  parameter_group_name = aws_db_parameter_group.this.id
  skip_final_snapshot  = true
  db_subnet_group_name = aws_db_subnet_group.this.name
  port                 = var.rds_port
}

output "rds_hostname" {
  description = "RDS instance hostname"
  value       = aws_db_instance.this.address
  sensitive   = true
}