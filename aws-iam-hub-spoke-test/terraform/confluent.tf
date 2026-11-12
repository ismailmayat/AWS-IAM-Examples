# -----------------------------------------------------------------------------
# Confluent Cloud: one AWS IAM identity provider for the hub issuer, one pool
# per workload pinned to the exact hub role ARN in `sub`.
# -----------------------------------------------------------------------------

data "confluent_kafka_cluster" "this" {
  id = var.confluent_cluster_id
  environment {
    id = var.confluent_environment_id
  }
}

resource "confluent_identity_provider" "hub" {
  display_name = "aws-iam-hub-${local.account_id}"
  description  = "AWS outbound identity federation, hub account ${local.account_id}"
  issuer       = local.issuer
  jwks_uri     = local.jwks_uri
}

resource "confluent_identity_pool" "hub_workload" {
  identity_provider {
    id = confluent_identity_provider.hub.id
  }
  display_name   = local.hub_role_name
  description    = "Hub role ${local.hub_role_arn}"
  identity_claim = "claims.sub"
  filter         = "claims.sub == \"${local.hub_role_arn}\" && claims.aud == \"${var.token_audience}\""
}

locals {
  cluster_crn       = "${data.confluent_kafka_cluster.this.rbac_crn}/kafka=${data.confluent_kafka_cluster.this.id}"
  bootstrap_servers = replace(data.confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://", "")
}

resource "confluent_role_binding" "topic_read" {
  principal   = "User:${confluent_identity_pool.hub_workload.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.cluster_crn}/topic=${var.topic_name}"
}

resource "confluent_role_binding" "group_read" {
  principal   = "User:${confluent_identity_pool.hub_workload.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.cluster_crn}/group=${var.consumer_group_id}"
}
