# Lake storage: one bucket, one prefix per layer, Glue databases, Athena.
#
#   s3://thelook-lake-<account>/bronze/          Iceberg tables written by the sink
#                              /silver/, /gold/  dbt (P3)
#                              /athena-results/  query results, expire after 7 days

locals {
  lake_bucket = "thelook-lake-${data.aws_caller_identity.current.account_id}"
  layers      = ["bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
  # One-command teardown (O7): destroy removes the data too. Bronze can be
  # rebuilt from a new Debezium snapshot; nothing here is the only copy of
  # source data.
  force_destroy = true
}

# No versioning, on purpose: Iceberg keeps its own snapshots, and S3 object
# versions would keep the old copy of every file an erasure rewrites (GDPR,
# O5). Deleted data must really disappear once snapshots are expired.

# SSE-KMS with the AWS-managed key aws/s3 (no $1/month customer key). The
# S3 Bucket Key derives a data key per bucket instead of calling KMS for
# every object, which cuts KMS request charges by ~99%.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms" # no key ID: S3 uses the aws/s3 managed key
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # The sink uploads large files in parts; a crash leaves invisible, billed
  # parts behind unless they are aborted.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Query results are re-creatable and may contain personal data.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

# --- Glue Data Catalog: one database per layer ----------------------------------
# Tables are not declared here: the Iceberg sink creates the bronze tables
# (ADR 006) and dbt the silver and gold ones. Deleting a database deletes
# its tables' metadata too.
resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(local.layers)
  name         = "thelook_${each.key}"
  description  = "theLook ${each.key} layer (Iceberg tables)"
  location_uri = "s3://${local.lake_bucket}/${each.key}/"
}

# --- Athena ---------------------------------------------------------------------
data "aws_kms_alias" "s3" {
  name = "alias/aws/s3"
}

resource "aws_athena_workgroup" "lake" {
  name          = "thelook"
  force_destroy = true

  configuration {
    # Clients cannot override these settings (result location, encryption,
    # scan limit): the limits hold for every query.
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    # Queries scanning more are cancelled: at $5/TB, 1 GiB is ~$0.005.
    # Lake tables are MBs; a hit means a missing filter or partition.
    bytes_scanned_cutoff_per_query = 1073741824

    # Engine version 3: needed for MERGE on Iceberg (dbt silver, P3).
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.lake_bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = data.aws_kms_alias.s3.target_key_arn
      }
    }
  }
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}
