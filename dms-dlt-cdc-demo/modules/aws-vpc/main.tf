data "aws_availability_zones" "available" {}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "3.14.2"

  name = "dms-rds-vpc"
  cidr = var.cidr_block
  azs  = data.aws_availability_zones.available.names

  private_subnets = [cidrsubnet(var.cidr_block, 3, 1),
  cidrsubnet(var.cidr_block, 3, 2)]

  default_security_group_egress = [{
    cidr_blocks = "0.0.0.0/0"
  }]

  default_security_group_ingress = [{
    description = "Allow all internal TCP and UDP"
    self        = true
  }]

}

output "private_subnets" {
  value = module.vpc.private_subnets
}

output "default_sg" {
  value = module.vpc.default_security_group_id
}