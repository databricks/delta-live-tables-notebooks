resource "aws_lambda_invocation" "this" {
  function_name = var.lambda_function_name
  qualifier = "$LATEST"

  input = <<JSON
  {
    "operation": "${var.operation}"
  }
  JSON
}