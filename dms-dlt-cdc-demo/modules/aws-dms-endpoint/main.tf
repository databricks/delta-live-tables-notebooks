# Create a new endpoint
resource "aws_dms_endpoint" "mysql" {
  database_name = var.rds_database
  endpoint_id   = "mysql-dms-endpoint-tf"
  endpoint_type = "source"
  engine_name   = "mysql"
  password      = var.rds_password
  port          = var.rds_port
  server_name   = var.rds_hostname
  ssl_mode      = "none"

  username = var.rds_username
}

resource "aws_s3_bucket" "this" {
  bucket = "${var.prefix}-dms-cdc-db"
  force_destroy = true
}

resource "aws_iam_role" "this" {
  name = "dms_s3_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "dms.amazonaws.com"
        }
      },
    ]
  })

  inline_policy {
    name = "my_inline_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
          {
            Action   = ["s3:*"]
            Effect   = "Allow"
            Resource = [aws_s3_bucket.this.arn, "${aws_s3_bucket.this.arn}/*"]
          },
        ]
      })
    }
}

resource "aws_dms_endpoint" "s3" {
  endpoint_id   = "s3-dms-endpoint-tf"
  endpoint_type = "target"
  engine_name   = "s3"
  s3_settings {
    service_access_role_arn = aws_iam_role.this.arn
    bucket_name             = aws_s3_bucket.this.id
    bucket_folder           = "data"
    add_column_name         = true
    cdc_max_batch_interval = 20
    data_format             = "csv"
    cdc_inserts_and_updates = true
    timestamp_column_name = "dmsTimestamp"
    rfc_4180                = true
  }

}

output "dms_endpoint_mysql_arn" {
  description = "RDS instance hostname"
  value       = aws_dms_endpoint.mysql.endpoint_arn
  sensitive   = true 
}

output "dms_endpoint_s3_arn" {
  description = "RDS instance hostname"
  value       = aws_dms_endpoint.s3.endpoint_arn
  sensitive   = true 
}

output "s3_bucket_arn" {
  value = aws_s3_bucket.this.arn
}

output "s3_bucket_name" {
  value = aws_s3_bucket.this.id
}