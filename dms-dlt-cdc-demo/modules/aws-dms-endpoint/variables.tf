variable "rds_hostname" {
    type    = string
    description = "RDS Hostname"
}

variable "rds_username" {
    type = string
    description = "username to use for the root rds user"
}

variable "rds_password" {
    type = string
    description = "password to use for the root rds user"
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

variable "prefix" {
    type = string
    description = "random prefix"
}