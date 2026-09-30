# The lake on AWS: S3, Glue, IAM, Athena, budget (P2 onwards).

terraform {
  required_version = "~> 1.16"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.66"
    }
  }

  # Remote state in the bootstrap bucket. The bucket name contains the
  # account ID, so it is passed at init time (make tf-init) instead of being
  # written here. use_lockfile: S3-native locking (Terraform >= 1.10), so two
  # applies cannot run at once; no DynamoDB table needed.
  backend "s3" {
    key          = "lake/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
