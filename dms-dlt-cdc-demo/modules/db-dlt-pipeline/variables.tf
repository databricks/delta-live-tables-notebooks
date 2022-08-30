variable "notebook_path" {
    type = string
    description = "DLT Notebook Path"
}

variable "instance_profile_arn" {
  type = string
  description = "Databricks Instance Profile"
}

variable "s3_data_path" {
  type = string
  description = "S3 path that dms data is going to"
}

variable "rds_database" {
    type = string
    description = "Database to create tables in"
    default = "databricks"
}

variable "prefix" {
    type = string
    description = "random prefix"
}