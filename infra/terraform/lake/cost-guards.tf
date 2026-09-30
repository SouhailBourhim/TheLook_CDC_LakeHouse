# Safeguards for spend the project budget (budget.tf) cannot see: anything
# without the project tag. The budget filters on the tag, so an untagged
# resource (created by hand, by a service, or where tags are unsupported)
# costs money invisibly. Three layers catch it:
#   1. an unfiltered account budget with a low threshold (below);
#   2. a weekly Cost Explorer report grouped by the tag
#      (scripts/cost-report.sh, make cost-report);
#   3. an AWS Config rule flagging S3 buckets without the tag (below).

# --- 1. Account-wide budget -----------------------------------------------------
# The account spent $0.00 in Aug and Sep 2026, so any charge is news. This is
# a 5th budget on the account (~$0.02/day), requested by Souhail.
resource "aws_budgets_budget" "account_total" {
  name         = "thelook-account-total-monthly"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Measure usage, not the bill: credits (e.g. free-plan credits) are applied
  # untagged at account level and would net the spend to ~$0, so the budget
  # would stay silent until the credits run out.
  cost_types {
    include_credit = false
    include_refund = false
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 1
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 5
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# --- 3. AWS Config: S3 buckets must carry the project tag -----------------------
# Config needs a recorder (one per region; none existed), a delivery channel
# to an S3 bucket, and a rule. Cost: ~$0.003 per recorded configuration item
# and ~$0.001 per rule evaluation. To keep it to cents, only S3 buckets are
# recorded: they hold this project's data and storage cost. In this shared
# account, other projects' buckets will show as NON_COMPLIANT too.

resource "aws_iam_service_linked_role" "config" {
  aws_service_name = "config.amazonaws.com"
}

resource "aws_s3_bucket" "config" {
  bucket        = "thelook-config-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # only Config snapshots; teardown must not get stuck
}

resource "aws_s3_bucket_public_access_block" "config" {
  bucket                  = aws_s3_bucket.config.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "config" {
  bucket = aws_s3_bucket.config.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "config" {
  bucket = aws_s3_bucket.config.id
  rule {
    id     = "expire-config-history"
    status = "Enabled"
    filter {}
    expiration {
      days = 90
    }
  }
}

# Config's service writes here; the SourceAccount condition stops another
# account's Config from using this bucket (confused-deputy protection).
resource "aws_s3_bucket_policy" "config" {
  bucket = aws_s3_bucket.config.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSConfigBucketPermissionsCheck"
        Effect    = "Allow"
        Principal = { Service = "config.amazonaws.com" }
        Action    = ["s3:GetBucketAcl", "s3:ListBucket"]
        Resource  = aws_s3_bucket.config.arn
        Condition = { StringEquals = { "AWS:SourceAccount" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "AWSConfigBucketDelivery"
        Effect    = "Allow"
        Principal = { Service = "config.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.config.arn}/AWSLogs/${data.aws_caller_identity.current.account_id}/Config/*"
        Condition = { StringEquals = { "AWS:SourceAccount" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.config.arn, "${aws_s3_bucket.config.arn}/*"]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
    ]
  })
  depends_on = [aws_s3_bucket_public_access_block.config]
}

resource "aws_config_configuration_recorder" "main" {
  name     = "thelook-recorder"
  role_arn = aws_iam_service_linked_role.config.arn

  recording_group {
    all_supported  = false
    resource_types = ["AWS::S3::Bucket"]
  }
}

resource "aws_config_delivery_channel" "main" {
  name           = "thelook-delivery"
  s3_bucket_name = aws_s3_bucket.config.bucket
  depends_on     = [aws_config_configuration_recorder.main, aws_s3_bucket_policy.config]
}

resource "aws_config_configuration_recorder_status" "main" {
  name       = aws_config_configuration_recorder.main.name
  is_enabled = true
  depends_on = [aws_config_delivery_channel.main]
}

# AWS managed rule: NON_COMPLIANT when a bucket lacks project=thelook-cdc-lakehouse.
resource "aws_config_config_rule" "required_tags" {
  name = "thelook-s3-required-project-tag"

  source {
    owner             = "AWS"
    source_identifier = "REQUIRED_TAGS"
  }

  input_parameters = jsonencode({
    tag1Key   = "project"
    tag1Value = "thelook-cdc-lakehouse"
  })

  scope {
    compliance_resource_types = ["AWS::S3::Bucket"]
  }

  depends_on = [aws_config_configuration_recorder_status.main]
}
