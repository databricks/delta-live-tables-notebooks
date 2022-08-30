resource "aws_iam_instance_profile" "s3_instance" {
  name = "dms_cdc_profile"
  role = aws_iam_role.s3_role.name
}

resource "aws_iam_role" "s3_role" {
  name               = "dms_cdc_role"
  path               = "/"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  inline_policy {
    name = "my_inline_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
          {
            Action   = ["s3:*"]
            Effect   = "Allow"
            Resource = [var.s3_bucket_arn, "${var.s3_bucket_arn}/*"]
          },
        ]
      })
  }
}

resource "aws_iam_policy" "pass_role" {
  name = "s3-pass-role"
  # role = aws_iam_role.s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "iam:PassRole",
        ]
        Effect   = "Allow"
        Resource = aws_iam_role.s3_role.arn
      },
    ]
  })
}

data "aws_iam_role" "crossaccount" {
  name = var.db_crossaccount_role
}

resource "aws_iam_role_policy_attachment" "attach_s3_role" {
  role       = data.aws_iam_role.crossaccount.id
  policy_arn = aws_iam_policy.pass_role.id
}

resource "databricks_instance_profile" "this" {
  instance_profile_arn = aws_iam_instance_profile.s3_instance.arn
  skip_validation = true
}

output "db_instance_profile" {
  value = databricks_instance_profile.this.id
}