variable "subnets" {
    type    = list(string)
    description = "Subnet used to deploy RDS Instance, VPC RDS deployed needs to have connectivity to DMS Replication Instance and Databricks"
}