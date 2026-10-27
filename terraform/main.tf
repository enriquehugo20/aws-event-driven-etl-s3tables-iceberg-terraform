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
# 1. El contenedor principal (S3 Table Bucket)
resource "aws_s3tables_table_bucket" "this" {
  name = "processed-${local.resource_suffix}"

  maintenance_configuration = {
    iceberg_unreferenced_file_removal = {
      status = "enabled"
      settings = {
        non_current_days  = 7
        unreferenced_days = 3
      }
    }
  }

  tags = {
    Environment = local.environment
    Project     = "csv-parquet-etl"
  }
}

# 2. El Namespace explícito que alojará tus esquemas
resource "aws_s3tables_namespace" "sales" {
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  namespace        = "sales"
}

# 3. La tabla Iceberg atada rígidamente al namespace mediante depends_on
resource "aws_s3tables_table" "sales" {
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  namespace        = aws_s3tables_namespace.sales.namespace
  name             = "sales"
  format           = "ICEBERG"

  maintenance_configuration = {
    iceberg_compaction = {
      status = "enabled"
      settings = {
        target_file_size_mb = 256
      }
    }
    iceberg_snapshot_management = {
      status = "enabled"
      settings = {
        max_snapshot_age_hours = 72
        min_snapshots_to_keep  = 2
      }
    }
  }

  metadata {
    iceberg {
      schema {
        field {
          name     = "transaction_date"
          type     = "date"
          required = false
        }
      }
    }
  }

  depends_on = [aws_s3tables_namespace.sales]
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