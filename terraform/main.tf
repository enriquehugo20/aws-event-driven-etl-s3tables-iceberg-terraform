# main.tf

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
    sales = {
      format    = "ICEBERG"
      namespace = "sales"

      partition_keys = [
        {
          name      = "transaction_date"
          type      = "date"
          transform = "identity"
        }
      ]

      maintenance_configuration = {
        iceberg_compaction = {
          status = "enabled"

          settings = {
            target_file_size_mb = 256
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

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
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