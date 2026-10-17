variable "alert_email" {
  description = "Where AWS Budgets and the alert topic send email. Set in terraform.tfvars (git-ignored)."
  type        = string
  # Redacted in plan output: CI's drift check runs in a public repository's
  # logs (ADR 022).
  sensitive = true
}
