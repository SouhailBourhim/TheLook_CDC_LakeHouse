#!/bin/sh
# Renders alertmanager.yml with the alert topic's ARN from onprem/.env, then
# starts Alertmanager on that copy (ADR 020). The ARN holds the AWS account
# ID, which the repository never commits, and Alertmanager does not expand
# environment variables in its configuration.
#
# The SNS receiver signs its requests with AWS's default credential chain:
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY of the thelook-alerts user, which
# may only publish to that topic.
set -eu
: "${ALERTS_TOPIC_ARN:?set ALERTS_TOPIC_ARN in onprem/.env}"
sed "s|__ALERTS_TOPIC_ARN__|$ALERTS_TOPIC_ARN|" /etc/alertmanager/alertmanager.yml \
  > /tmp/alertmanager.yml
exec /bin/alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager
