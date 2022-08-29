variable "rds_username" {
    type = string
    description = "username to use for the root rds user"
    default = "databricks_root"
}

variable "rds_password" {
    type = string
    description = "password to use for the root rds password, rds spun up is in private subnet"
    default = "databricks1"
}

variable "rds_database" {
    type = string
    description = "Database to create tables in"
    default = "databricks"
}

variable "rds_port" {
    type = number
    description = "Database port"
    default = 3306
}

variable "db_crossaccount_role" {
    type = string
    description = "name of cross account role used for Databricks"
}

variable "tags" {
    type = list(string)
    description = "Default Tags to identify resource"
    default = ["Databricks", "DMS CDC DLT DEMO"]
}

variable "region" {
    type = string
    description = "AWS Region"
}