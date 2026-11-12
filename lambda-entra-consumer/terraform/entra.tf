# -----------------------------------------------------------------------------
# Entra ID app registration
#
# One app registration plays both OAuth roles:
#   - client   : the Lambda ESM authenticates as it (client_id + client_secret)
#   - resource : the token is requested for its own App ID URI
#                (scope = api://<client_id>/.default), so the token's `aud`
#                is this app's client ID.
#
# v2 access tokens are required so that the issuer is
# https://login.microsoftonline.com/<tenant>/v2.0 (matching the Confluent
# identity provider) and the `azp` claim is present.
# -----------------------------------------------------------------------------

# The identity running Terraform. It must be an explicit owner: without that,
# a non-admin user can create the app but gets 403 on every later change
# (identifier URI, service principal, password).
data "azuread_client_config" "current" {}

resource "azuread_application" "consumer" {
  display_name     = "${var.name_prefix}-confluent-cloud"
  sign_in_audience = "AzureADMyOrg"
  owners           = [data.azuread_client_config.current.object_id]

  api {
    requested_access_token_version = 2
  }
}

# App ID URI api://<client_id>. This is a separate resource because the URI
# references the app's own client ID.
resource "azuread_application_identifier_uri" "consumer" {
  application_id = azuread_application.consumer.id
  identifier_uri = "api://${azuread_application.consumer.client_id}"
}

resource "azuread_service_principal" "consumer" {
  client_id = azuread_application.consumer.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

# Client secret that is rotated on a schedule: once time_rotating expires, the
# next `terraform apply` creates a new secret and updates Secrets Manager.
# The Lambda ESM picks up the new value without being recreated.
resource "time_rotating" "client_secret" {
  rotation_days = var.client_secret_rotation_days
}

resource "azuread_application_password" "consumer" {
  application_id = azuread_application.consumer.id
  display_name   = "lambda-esm"
  end_date       = timeadd(time_rotating.client_secret.rotation_rfc3339, "168h") # 7-day overlap

  rotate_when_changed = {
    rotation = time_rotating.client_secret.id
  }
}

locals {
  entra_issuer         = "https://login.microsoftonline.com/${var.entra_tenant_id}/v2.0"
  entra_token_endpoint = "https://login.microsoftonline.com/${var.entra_tenant_id}/oauth2/v2.0/token"
  entra_scope          = "${azuread_application_identifier_uri.consumer.identifier_uri}/.default"
}
