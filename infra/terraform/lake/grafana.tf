# Grafana's identity in AWS (P9b, ADR 023): Athena queries for the business
# dashboard, on the gold MARTS only. The marts are aggregates (no personal
# data); the rest of gold holds dim_user (addresses), silver and bronze hold
# everything. If the key in onprem/.env leaks, it reads daily revenue,
# funnel and ratings, nothing else, and writes only query results.
#
# The access key is created with the AWS CLI, as for the other users
# (runbook, "Spark AWS credentials"): never in the Terraform state.

resource "aws_iam_user" "grafana" {
  name = "thelook-grafana"
  path = "/thelook/"
}

data "aws_iam_policy_document" "grafana" {
  # Athena calls in the thelook workgroup, which enforces the results
  # location, encryption and the 1 GiB scan cutoff per query.
  statement {
    sid = "QueryInTheWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.lake.arn]
  }

  # The plugin's query editor lists catalogs, databases and tables.
  statement {
    sid = "BrowseTheCatalog"
    actions = [
      "athena:GetDataCatalog",
      "athena:ListDatabases",
      "athena:GetDatabase",
      "athena:ListTableMetadata",
      "athena:GetTableMetadata",
    ]
    resources = ["arn:aws:athena:us-east-1:${data.aws_caller_identity.current.account_id}:datacatalog/AwsDataCatalog"]
  }

  statement {
    sid       = "ListCatalogsAndWorkgroups"
    actions   = ["athena:ListDataCatalogs", "athena:ListWorkGroups"]
    resources = ["*"]
  }

  # Glue: the gold database, and only the mart tables in it.
  statement {
    sid     = "ReadMartsCatalog"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables"]
    resources = [
      "${local.glue_prefix}:catalog",
      aws_glue_catalog_database.layer["gold"].arn,
      "${local.glue_prefix}:table/${aws_glue_catalog_database.layer["gold"].name}/mart_*",
    ]
  }

  statement {
    sid       = "ListMartsAndResults"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["gold/mart_*", "athena-results/", "athena-results/*"]
    }
  }

  statement {
    sid       = "LocateTheBucket"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }

  # Each mart's data and Iceberg metadata live under gold/<mart>/.
  statement {
    sid       = "ReadTheMarts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/gold/mart_*"]
  }

  # Athena writes each result with the caller's credentials, then reads it.
  statement {
    sid       = "WriteQueryResults"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/athena-results/*"]
  }
}

resource "aws_iam_policy" "grafana" {
  name        = "thelook-grafana"
  path        = "/thelook/"
  description = "Grafana: read-only Athena SQL on the gold marts (thelook workgroup)"
  policy      = data.aws_iam_policy_document.grafana.json
}

resource "aws_iam_user_policy_attachment" "grafana" {
  user       = aws_iam_user.grafana.name
  policy_arn = aws_iam_policy.grafana.arn
}
