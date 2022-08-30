data "aws_iam_policy_document" "this"{
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  publish = true
  function_name = "rdsDataRunner"
  description   = "My awesome lambda function"
  handler       = "handler.lambda_handler"

  source_path = "../../resources/lambda/python"
  runtime = "python3.8"
  timeout = 300

  environment_variables = {
      DB_USER = var.rds_username
      DB_PASSWORD = var.rds_password
      DB_HOSTNAME = var.rds_hostname
      DB_PORT     = var.rds_port
  }

  vpc_subnet_ids         = var.subnets
  vpc_security_group_ids = var.security_group_ids

  number_of_policies = 2
  attach_policies = true
  policies           = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    ]

  assume_role_policy_statements = {
    account_root = {
      effect  = "Allow",
      actions = ["sts:AssumeRole"],
      principals = {
        account_principal = {
          type        = "Service",
          identifiers = ["lambda.amazonaws.com"]
        }
      }
    }
  }
}

output "name" {
  value = module.lambda_function.lambda_function_name
}