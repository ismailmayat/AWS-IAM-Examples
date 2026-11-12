terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.70.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.0.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

# CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET (needs rights to create
# an identity provider, identity pools and role bindings).
provider "confluent" {}
