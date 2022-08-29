variable "dms_replication_instance_arn" {
    type    = string
    description = "DMS replication Instance ARN"
}

variable "dms_endpoint_mysql_arn" {
    type    = string
    description = "RDS DMS Endpoint ARN"
}

variable "dms_endpoint_s3_arn" {
    type    = string
    description = "S3 DMS Endpoint ARN"
}