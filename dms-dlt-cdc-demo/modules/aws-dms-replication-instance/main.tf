data "aws_iam_policy_document" "dms_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["dms.amazonaws.com"]
      type        = "Service"
    }
  }
}

resource "aws_iam_role" "dms-access-for-endpoint" {
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json
  name               = "dms-access-for-endpoint"
}

resource "aws_iam_role" "dms-cloudwatch-logs-role" {
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json
  name               = "dms-cloudwatch-logs-role"
}

resource "aws_iam_role_policy_attachment" "dms-cloudwatch-logs-role-AmazonDMSCloudWatchLogsRole" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole"
  role       = aws_iam_role.dms-cloudwatch-logs-role.name
}

resource "aws_iam_role" "dms-vpc-role" {
  assume_role_policy = data.aws_iam_policy_document.dms_assume_role.json
  name               = "dms-vpc-role"
}

resource "aws_iam_role_policy_attachment" "dms-vpc-role-AmazonDMSVPCManagementRole" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
  role       = aws_iam_role.dms-vpc-role.name

  # It takes some time for these attachments to work, and creating the aws_dms_replication_subnet_group fails if this attachment hasn't completed.
  provisioner "local-exec" {
    command = "sleep 30"
  }
}

# Create a new replication subnet group
resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_description = "Replication subnet group"
  replication_subnet_group_id          = "dms-replication-subnet-group-tf"
  subnet_ids = var.subnets

  depends_on = [
    aws_iam_role_policy_attachment.dms-vpc-role-AmazonDMSVPCManagementRole
  ]
}

# Create a new replication instance
resource "aws_dms_replication_instance" "this" {
  allocated_storage            = 20
  apply_immediately            = true
  auto_minor_version_upgrade   = true
  multi_az                     = false
  preferred_maintenance_window = "sun:10:30-sun:14:30"
  replication_instance_class   = "dms.t2.medium"
  replication_instance_id      = "dms-replication-instance-tf"
  replication_subnet_group_id  = aws_dms_replication_subnet_group.this.id

  depends_on = [
    aws_iam_role_policy_attachment.dms-cloudwatch-logs-role-AmazonDMSCloudWatchLogsRole,
    aws_iam_role_policy_attachment.dms-vpc-role-AmazonDMSVPCManagementRole,
  ]
}

output "dms_replication_instance_arn" {
  description = "RDS instance hostname"
  value       = aws_dms_replication_instance.this.replication_instance_arn
  sensitive   = true 
}