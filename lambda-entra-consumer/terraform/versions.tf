terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # 6.70.0 is the first release whose Lambda SDK accepts the
      # OAUTHBEARER_* source access configuration types.
      version = ">= 6.70.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = ">= 3.0.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.0.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4.0"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.9.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

# Authenticates with `az login` (or ARM_* environment variables).
provider "azuread" {
  tenant_id = var.entra_tenant_id
}

# Authenticates with the CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
# environment variables (a Cloud API key with OrganizationAdmin, or at least
# rights to manage identity providers/pools and role bindings).
provider "confluent" {}
