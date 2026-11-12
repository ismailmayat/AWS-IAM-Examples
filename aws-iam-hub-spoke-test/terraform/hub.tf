# -----------------------------------------------------------------------------
# HUB side (in a real deployment: the kafka-identity account).
# In this single-account test the hub and spoke are the same account.
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_organizations_organization" "current" {}

# Outbound federation is a one-time account-level setting, enabled outside
# Terraform (see README prerequisites); only the issuer is read here.
data "aws_iam_outbound_web_identity_federation" "hub" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  hub_role_name = "${var.environment}-${var.workload}"
  hub_role_path = "/kafka/"
  # Built as a string so the hub trust policy and the spoke's AssumeRole
  # permission can reference each other without a dependency cycle.
  hub_role_arn = "arn:aws:iam::${local.account_id}:role${local.hub_role_path}${local.hub_role_name}"

  issuer   = data.aws_iam_outbound_web_identity_federation.hub.issuer_identifier
  jwks_uri = "${data.aws_iam_outbound_web_identity_federation.hub.issuer_identifier}/.well-known/jwks.json"
}

# Trust: pin the exact runtime role, never the whole account.
data "aws_iam_policy_document" "hub_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:SetSourceIdentity"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.runtime.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalOrgID"
      values   = [data.aws_organizations_organization.current.id]
    }
  }
}

# Permissions: one audience, capped lifetime, no caller-supplied tags.
data "aws_iam_policy_document" "hub_mint" {
  statement {
    actions   = ["sts:GetWebIdentityToken"]
    resources = ["*"]
    condition {
      test     = "ForAllValues:StringEquals"
      variable = "sts:IdentityTokenAudience"
      values   = [var.token_audience]
    }
    condition {
      test     = "NumericLessThanEquals"
      variable = "sts:DurationSeconds"
      values   = [tostring(var.token_max_duration_seconds)]
    }
  }
}

resource "aws_iam_role" "hub" {
  name               = local.hub_role_name
  path               = local.hub_role_path
  assume_role_policy = data.aws_iam_policy_document.hub_trust.json

  tags = local.hub_role_tags
}

# Required hub-role tags from the design; they should surface in the JWT as
# principal tags and keep the source account visible in audit.
# IAM tag keys are case-insensitive: if var.tags (provider default_tags)
# already has e.g. "Owner", the role must use that exact spelling to
# override it, otherwise CreateRole fails with "Duplicate tag keys".
locals {
  hub_role_tags_wanted = {
    source-account = local.account_id
    workload       = var.workload
    environment    = var.environment
    owner          = var.owner
  }
  default_tag_key_by_lower = { for k in keys(var.tags) : lower(k) => k }
  hub_role_tags = {
    for k, v in local.hub_role_tags_wanted :
    lookup(local.default_tag_key_by_lower, lower(k), k) => v
  }
}

resource "aws_iam_role_policy" "hub_mint" {
  name   = "mint-confluent-token"
  role   = aws_iam_role.hub.id
  policy = data.aws_iam_policy_document.hub_mint.json
}
