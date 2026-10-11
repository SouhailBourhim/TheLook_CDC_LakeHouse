# --- Alert delivery (P7, ADR 020) ------------------------------------------------
# Alertmanager is the single alert hub; it publishes every notification to
# this topic, which emails alert_email (the address the budgets already use).
# The email subscription stays "pending confirmation" until the link AWS
# sends is clicked: SNS delivers nothing before that.
#
# Not encrypted with KMS: messages carry alert names and labels, never data,
# and an encrypted topic would make every publisher need KMS rights as well.
resource "aws_sns_topic" "alerts" {
  name = "thelook-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Alertmanager's identity: it may publish to this one topic and nothing else.
# Its key reaches the container through onprem/.env (Alertmanager's SNS
# client falls back to AWS's default credential chain).
resource "aws_iam_user" "alerts" {
  name = "thelook-alerts"
  path = "/thelook/"
}

data "aws_iam_policy_document" "alerts" {
  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_policy" "alerts" {
  name        = "thelook-alerts"
  path        = "/thelook/"
  description = "Publish to the thelook-alerts topic only (Alertmanager)"
  policy      = data.aws_iam_policy_document.alerts.json
}

resource "aws_iam_user_policy_attachment" "alerts" {
  user       = aws_iam_user.alerts.name
  policy_arn = aws_iam_policy.alerts.arn
}

# Holds the account ID, so it is read from Terraform into onprem/.env, never
# committed: terraform -chdir=infra/terraform/lake output -raw alerts_topic_arn
output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
