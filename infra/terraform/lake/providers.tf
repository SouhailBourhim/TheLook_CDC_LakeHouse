# Credentials come from AWS_PROFILE locally (make targets) and from OIDC in
# CI later; never from code.
provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      project    = "thelook-cdc-lakehouse"
      managed-by = "terraform/lake"
    }
  }
}

data "aws_caller_identity" "current" {}
