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
  region = "us-east-1" # O la región que estés utilizando
}

# Obtiene automáticamente tu Account ID de AWS
data.aws_caller_identity.current {}

locals {
  resource_suffix = "csv-parquet-etl-${data.aws_caller_identity.current.account_id}"
  environment     = "dev"
}

# ==========================================
# 1. BUCKETS TRADICIONALES (Raw y State)
# ==========================================
resource "aws_s3_bucket" "raw" {
  bucket        = "raw-${local.resource_suffix}"
  force_destroy = true

  tags = {
    Environment = local.environment
    Project     = "csv-parquet-etl"
  }
}

resource "aws_s3_bucket" "state" {
  bucket        = "state-${local.resource_suffix}"
  force_destroy = true

  tags = {
    Environment = local.environment
    Project     = "csv-parquet-etl"
  }
}

# ==========================================
# 2. S3 TABLE BUCKET (Con Módulo Oficial, Iceberg y Particionado)
# ==========================================
module "table_bucket" {
  source = "terraform-aws-modules/s3-bucket/aws//modules/table-bucket"

  # Usamos nuestro sufijo determinista para evitar colisiones globales
  table_bucket_name = "processed-${local.resource_suffix}"

  encryption_configuration = {
    sse_algorithm = "AES256"
  }

  maintenance_configuration = {
    iceberg_unreferenced_file_removal = {
      status = "enabled"

      settings = {
        non_current_days  = 7
        unreferenced_days = 3
      }
    }
  }

  tables = {
    Sales = {
      format    = "ICEBERG"
      namespace = "sales"

      # Particionado inteligente para acelerar consultas y reducir costos en Athena
      partition_keys = [
        {
          name      = "transaction_date"
          type      = "date"
          transform = "identity" # O puedes usar transformaciones como 'day' o 'month' si prefieres agruparlas
        }
      ]

      maintenance_configuration = {
        iceberg_compaction = {
          status = "enabled"

          settings = {
            target_file_size_mb = 256 # Sweet spot Athena
          }
        }
      }
    }
  }

  tags = {
    Environment = local.environment
    Project     = "csv-parquet-etl"
  }
}

# ==========================================
# 3. ROLES DE IAM CON SUFIJO DETERMINISTA
# ==========================================
resource "aws_iam_role" "glue_role" {
  name = "glue-role-${local.resource_suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = local.environment
  }
}

# Adjuntar política estándar de Glue al rol
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role" "eventbridge_role" {
  name = "eventbridge-role-${local.resource_suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = local.environment
  }
}

# ==========================================
# 4. GLUE JOB (Con sufijo determinista)
# ==========================================
resource "aws_glue_job" "etl_job" {
  name     = "glue-job-${local.resource_suffix}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.state.bucket}/scripts/etl_script.py"
  }

  default_arguments = {
    "--job-language" = "python"
  }

  tags = {
    Environment = local.environment
  }
}