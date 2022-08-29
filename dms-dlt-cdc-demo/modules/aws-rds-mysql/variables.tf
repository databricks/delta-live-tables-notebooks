variable "subnets" {
    type    = list(string)
    description = "Subnet used to deploy RDS Instance, VPC RDS deployed needs to have connectivity to DMS Replication Instance and Databricks"
}

variable "rds_username" {
    type = string
    description = "username to use for the root rds user"
}

variable "rds_password" {
    type = string
    description = "password to use for the root rds user"
}

variable "rds_port" {
    type = number
    description = "Database port"
    default = 3306
}

variable "rds_database" {
    type = string
    description = "Database to create tables in"
    default = "databricks"
}