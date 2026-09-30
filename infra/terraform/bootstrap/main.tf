# Bootstrap: the S3 bucket that holds the Terraform state of the `lake`
# configuration. It cannot store its own state in a bucket it has not created
# yet, so this small configuration keeps a LOCAL state (git-ignored). If that
# file is lost, re-import the bucket:
#   terraform import aws_s3_bucket.tfstate thelook-tfstate-<account id>

terraform {
  required_version = "~> 1.16"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.66"
    }
  }
}

# Credentials come from AWS_PROFILE (set by the Makefile), never from code.
provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      project    = "thelook-cdc-lakehouse"
      managed-by = "terraform/bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

# Bucket names are global across all AWS accounts; the account ID makes it unique.
resource "aws_s3_bucket" "tfstate" {
  bucket = "thelook-tfstate-${data.aws_caller_identity.current.account_id}"

  # Deleting this bucket would orphan every resource Terraform manages.
  lifecycle {
    prevent_destroy = true
  }
}

# Every state write keeps the previous version: a corrupted or wrongly
# applied state can be rolled back.
resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Old state versions are small, but keep the bucket from growing forever.
resource "aws_s3_bucket_lifecycle_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# State can contain secrets (e.g. generated passwords): encrypted at rest,
# never public, reachable only over TLS.
resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    object_ownership = "BucketOwnerEnforced" # ACLs disabled; IAM decides access
  }
}

resource "aws_s3_bucket_policy" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.tfstate.arn, "${aws_s3_bucket.tfstate.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.tfstate]
}

output "state_bucket" {
  value = aws_s3_bucket.tfstate.bucket
}
