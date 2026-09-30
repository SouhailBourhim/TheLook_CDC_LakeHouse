# Cost guard for this project only (spec O6: < $15/month; NFR cost).
#
# The account is shared with other projects, so the budget counts only
# resources tagged project=thelook-cdc-lakehouse (every resource gets the tag
# through the provider's default_tags). A user-defined tag must first be
# activated as a cost allocation tag; AWS only lists a tag key about 24 h
# after the first tagged resource exists.
resource "aws_ce_cost_allocation_tag" "project" {
  tag_key = "project"
  status  = "Active"
}

# The account already has 3 budgets and only 2 are free: this one costs
# about $0.02/day, accepted by Souhail for a project-scoped cost guard.
resource "aws_budgets_budget" "monthly" {
  name         = "thelook-cdc-lakehouse-monthly"
  budget_type  = "COST"
  limit_amount = "15"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$thelook-cdc-lakehouse"]
  }

  # $1 actual: the lake should cost cents; anything above means something runs
  # that should not (early warning, well before real money).
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
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  # The month is on course to break O6: act before it happens.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 15
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }

  depends_on = [aws_ce_cost_allocation_tag.project]
}
