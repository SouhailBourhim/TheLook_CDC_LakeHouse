# GitHub Actions' identity in AWS (NFR security, P9, ADR 022): a role that
# CI assumes with a short-lived OIDC token from GitHub, so no AWS key is
# stored in GitHub. It is READ-ONLY: CI runs `terraform plan` to detect
# drift (resources changed outside Terraform); applying stays manual
# (make tf-plan, then make tf-apply), so CI holds nothing that can change
# AWS.

# The account's GitHub OIDC provider already exists, created by another
# project (tag project=signal); AWS allows one per URL. Referenced, not
# managed: a `terraform destroy` here must not delete it under that project.
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

locals {
  github_repository = "SouhailBourhim/TheLook_CDC_LakeHouse"
  tfstate_bucket    = "thelook-tfstate-${data.aws_caller_identity.current.account_id}"
}

# Who may assume the role: a token GitHub issued for this repository's main
# branch (pushes and the scheduled run), for AWS (aud). Pull requests, other
# branches and forks get a different `sub` and are refused.
data "aws_iam_policy_document" "ci_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${local.github_repository}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "ci_drift" {
  name               = "thelook-ci-drift"
  path               = "/thelook/"
  description        = "GitHub Actions (main branch): read-only terraform plan for drift detection"
  assume_role_policy = data.aws_iam_policy_document.ci_trust.json
  # A plan takes about a minute; the token cannot be reused for long.
  max_session_duration = 3600
}

# What `terraform plan` reads: the state, then each managed resource's
# configuration (refresh). Configuration only: no lake data. S3 Get*/List*
# are granted on bucket ARNs, which cover the bucket settings APIs; objects
# have their own ARNs (bucket/*), so GetObject on lake data is not granted.
data "aws_iam_policy_document" "ci_drift" {
  statement {
    sid       = "ReadState"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${local.tfstate_bucket}/lake/terraform.tfstate"]
  }

  statement {
    sid     = "BucketSettings"
    actions = ["s3:GetBucket*", "s3:GetLifecycleConfiguration", "s3:GetEncryptionConfiguration", "s3:GetReplicationConfiguration", "s3:GetAccelerateConfiguration", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.lake.arn,
      aws_s3_bucket.config.arn,
      "arn:aws:s3:::${local.tfstate_bucket}",
    ]
  }

  # IAM metadata (users, policies, roles, the OIDC provider). IAM never
  # returns secret material (an access key's secret is not readable).
  statement {
    sid       = "IamMetadata"
    actions   = ["iam:Get*", "iam:List*"]
    resources = ["*"]
  }

  statement {
    sid     = "CatalogAndQuery"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTags", "athena:GetWorkGroup", "athena:ListTagsForResource"]
    resources = concat(
      ["arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layer : db.arn],
      [aws_athena_workgroup.lake.arn],
    )
  }

  # These services have no resource-level permissions for their read calls
  # (or only "*"): read-only describe actions.
  statement {
    sid = "DescribeOthers"
    actions = [
      "kms:DescribeKey",
      "kms:ListAliases",
      "budgets:ViewBudget",
      "budgets:ListTagsForResource",
      "ce:ListCostAllocationTags",
      "config:Describe*",
      "config:ListTagsForResource",
      "sns:GetTopicAttributes",
      "sns:GetSubscriptionAttributes",
      "sns:ListTagsForResource",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ci_drift" {
  name   = "terraform-plan-read-only"
  role   = aws_iam_role.ci_drift.id
  policy = data.aws_iam_policy_document.ci_drift.json
}
