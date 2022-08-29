variable "s3_bucket_arn" {
    type    = string
    description = "Bucket to create Instance Profile Read Write"
}

variable "db_crossaccount_role" {
    type    = string
    description = "Databricks Cross Account Role"
}