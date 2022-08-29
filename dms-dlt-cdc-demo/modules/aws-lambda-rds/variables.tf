variable "subnets" {
    type    = list(string)
    description = "Subnet used to deploy lambda that has access to RDS"
}
variable "security_group_ids" {
    type    = list(string)
    description = "Security Group for Lambda, needs access to RDS in subnets"
}

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