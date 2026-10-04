# Workload identities for the on-prem Spark jobs (NFR security, ADR 007).
#
# One IAM user per job, each limited to the layer it writes: if a key leaks,
# the damage stays in that layer. P3 adds the streaming bronze writer; the
# batch jobs (read bronze, write silver and gold) have their own user below.
#
# The access key is NOT created here: aws_iam_access_key would store the
# secret in the Terraform state. It is created once with the AWS CLI and
# written to onprem/.env (see docs/runbook.md, "Spark AWS credentials").
#
# No KMS permission: the bucket uses the AWS-managed key aws/s3, whose key
# policy lets principals of this account use it through S3.

resource "aws_iam_user" "spark_stream" {
  name = "thelook-spark-stream"
  # A path groups the project's users; it appears in their ARN.
  path = "/thelook/"
}

data "aws_iam_policy_document" "spark_stream" {
  # Listing, only under bronze/. Without s3:ListBucket, S3 answers 403
  # instead of 404 for a missing object.
  statement {
    sid       = "ListBronze"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/", "bronze/*"]
    }
  }

  # Iceberg data and metadata files. Delete: Iceberg removes the files of a
  # commit that failed. AbortMultipartUpload: S3FileIO uploads large files
  # in parts and aborts them on failure.
  statement {
    sid = "ReadWriteBronzeObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*"]
  }

  # Iceberg's GlueCatalog keeps the table pointer (metadata location) in
  # Glue; commits are UpdateTable calls with Glue's optimistic version check.
  # Glue table actions need the catalog and database ARNs as well as the
  # table's. No DeleteTable: dropping a table is an admin action.
  statement {
    sid = "BronzeCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.layer["bronze"].arn,
      "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.layer["bronze"].name}/*",
    ]
  }
}

resource "aws_iam_policy" "spark_stream" {
  name        = "thelook-spark-stream"
  path        = "/thelook/"
  description = "Streaming bronze writer: read/write s3://<lake>/bronze/ and Glue thelook_bronze"
  policy      = data.aws_iam_policy_document.spark_stream.json
}

resource "aws_iam_user_policy_attachment" "spark_stream" {
  user       = aws_iam_user.spark_stream.name
  policy_arn = aws_iam_policy.spark_stream.arn
}

output "spark_stream_user" {
  value = aws_iam_user.spark_stream.name
}

# --- Batch jobs (P4): bronze -> silver -> gold -----------------------------------
# Reads bronze, writes silver and gold. Cannot write bronze (the stream's
# job) and cannot drop tables.
resource "aws_iam_user" "spark_batch" {
  name = "thelook-spark-batch"
  path = "/thelook/"
}

locals {
  glue_prefix = "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}"
}

data "aws_iam_policy_document" "spark_batch" {
  statement {
    sid       = "ListLayers"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/", "bronze/*", "silver/", "silver/*", "gold/", "gold/*"]
    }
  }

  statement {
    sid       = "ReadBronzeObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*"]
  }

  # Delete: Iceberg removes the files of a failed commit; merge-on-read and
  # table replacement write new files and drop references to old ones.
  statement {
    sid = "ReadWriteSilverGoldObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = [
      "${aws_s3_bucket.lake.arn}/silver/*",
      "${aws_s3_bucket.lake.arn}/gold/*",
    ]
  }

  statement {
    sid     = "ReadBronzeCatalog"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables"]
    resources = [
      "${local.glue_prefix}:catalog",
      aws_glue_catalog_database.layer["bronze"].arn,
      "${local.glue_prefix}:table/${aws_glue_catalog_database.layer["bronze"].name}/*",
    ]
  }

  # Gold dimensions are replaced atomically (REPLACE TABLE = UpdateTable in
  # Iceberg's GlueCatalog), so no DeleteTable is needed.
  statement {
    sid = "WriteSilverGoldCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "${local.glue_prefix}:catalog",
      aws_glue_catalog_database.layer["silver"].arn,
      aws_glue_catalog_database.layer["gold"].arn,
      "${local.glue_prefix}:table/${aws_glue_catalog_database.layer["silver"].name}/*",
      "${local.glue_prefix}:table/${aws_glue_catalog_database.layer["gold"].name}/*",
    ]
  }
}

resource "aws_iam_policy" "spark_batch" {
  name        = "thelook-spark-batch"
  path        = "/thelook/"
  description = "Batch jobs: read bronze; read/write silver and gold (S3 and Glue)"
  policy      = data.aws_iam_policy_document.spark_batch.json
}

resource "aws_iam_user_policy_attachment" "spark_batch" {
  user       = aws_iam_user.spark_batch.name
  policy_arn = aws_iam_policy.spark_batch.arn
}

output "spark_batch_user" {
  value = aws_iam_user.spark_batch.name
}
