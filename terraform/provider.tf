# provider.tf

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.75.0"
    }
  }

  # Backend Remoto S3 para persistir el estado fuera de GitHub Actions
  backend "s3" {
    bucket         = "tf-state-csv-parquet-etl-276846898236" # Único gracias a tu account ID
    key            = "etl/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

locals {
  resource_suffix = "csv-parquet-etl-v4-${data.aws_caller_identity.current.account_id}" # Cambiamos a v4
  environment     = "dev"
}