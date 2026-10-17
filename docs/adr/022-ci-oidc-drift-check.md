# 022. CI on AWS through OIDC: a read-only drift check

- Status: Proposed
- Date: 2026-10-17 (P9; spec FR18, NFR security)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The spec asks for CI that runs `terraform fmt -check` and `terraform
validate` (FR18) and for GitHub Actions to authenticate to AWS with OIDC
(NFR security). Format and validation need no AWS access (`init
-backend=false`) and run in `ci.yml` since P9 step 12. OIDC only makes
sense if CI has something to do in AWS, and nothing so far required it.

Facts found in P9:

- The account already has GitHub's OIDC provider
  (`token.actions.githubusercontent.com`), created by another project
  (tag `project=signal`). AWS allows one provider per URL.
- The repository is public: Actions logs are readable by anyone. A plan's
  diff prints resource attributes, including ARNs (the account id) and the
  alert email (a Terraform variable).
- The lake's state lives in S3 with S3-native locking (`use_lockfile`):
  a normal plan writes a lock file.

## Options considered (what CI does in AWS)

1. **Nothing (no OIDC).** Leaves the NFR unmet; no drift detection.
2. **Plan on pull requests, apply on merge (full CD).** The usual pattern
   in teams. Here: a role that can change IAM, S3 and the budget would be
   assumable by any workflow run on `main`; one person works on the repo
   and already reviews each plan locally (`make tf-plan`, then `make
   tf-apply` of that exact plan). A standing write role adds risk and no
   review.
3. **A read-only drift check (chosen).** A weekly and on-push `terraform
   plan -detailed-exitcode` (0 = no change, 2 = drift). It answers a real
   question: does AWS still match the code? (A hand change in the console,
   or a merged change nobody applied.)

## Decision

- **Role `thelook-ci-drift`** (`infra/terraform/lake/ci.tf`), assumable
  only with a GitHub OIDC token whose `aud` is `sts.amazonaws.com` and
  whose `sub` is
  `repo:SouhailBourhim@184526894/TheLook_CDC_LakeHouse@1396083316:ref:refs/heads/main`
  (pushes and the schedule; pull requests, other branches and forks get a
  different `sub`). Sessions last at most 1 hour. The repository uses
  GitHub's immutable subject (owner and repository ids next to the names):
  a repository created later under the same name has other ids and is
  refused. Found by the first run, refused against the name-only form.
- **Read-only, configuration only:** the state object; bucket settings on
  the three buckets' ARNs (object ARNs are different, so no lake data);
  IAM metadata (IAM never returns secrets); Glue databases and the Athena
  workgroup; describe calls for KMS, Budgets, Cost Explorer tags, Config
  and SNS. No write action anywhere.
- **The existing OIDC provider is referenced (data source), not managed:**
  a `terraform destroy` of the lake must not delete it under the other
  project.
- **Public logs:** the workflow prints only which resources differ and the
  action (`aws_s3_bucket.lake would be update`), never the plan; the
  account id is masked (`::add-mask::`); the role ARN and the email are
  repository secrets; `alert_email` is a sensitive variable.
- **`plan -lock=false`:** the role cannot write the lock file, and a plan
  changes nothing.
- Separate workflow (`drift.yml`): only it may request an OIDC token
  (`id-token: write`); `ci.yml` keeps `contents: read`.
- The bootstrap configuration (local state) is not checked: its one
  bucket rarely changes and its state is not in S3.

## Consequences

- ✅ No AWS key stored in GitHub; credentials last an hour and are bound to
  this repository's main branch.
- ✅ Drift is found within a week (or at the push that causes it).
- ✅ Nothing in CI can change AWS.
- ⚠️ Applying stays manual, so a merged change to `infra/` turns the drift
  check red until someone applies it. That is the point: it is visible.
- ⚠️ The role depends on the other project's OIDC provider. If that project
  deletes it, the drift check fails at the credentials step; recreating it
  (one resource) fixes both.
- ⚠️ A new resource type in the lake needs its read actions added to the
  policy, or the plan fails with AccessDenied (the failure names the
  action).

## References

- GitHub, Configuring OpenID Connect in Amazon Web Services:
  docs.github.com/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services
- AWS IAM, Create an OpenID Connect identity provider:
  docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_oidc.html
- Terraform, `plan -detailed-exitcode`:
  developer.hashicorp.com/terraform/cli/commands/plan
