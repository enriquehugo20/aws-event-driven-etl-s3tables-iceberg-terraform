# provider.tf

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.75.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# Declarado aquí de forma global para que cualquier archivo del módulo raíz pueda usarlo
data "aws_caller_identity" "current" {}

locals {
  resource_suffix = "csv-parquet-etl-${data.aws_caller_identity.current.account_id}"
  environment     = "dev"
}