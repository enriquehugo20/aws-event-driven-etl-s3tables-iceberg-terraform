data "aws_caller_identity" "current" {}

locals {
  resource_suffix = "csv-parquet-etl-v6-${data.aws_caller_identity.current.account_id}"
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

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
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
# 4. BUCKET DEDICADO PARA SCRIPTS Y LOGS DE GLUE
# ==========================================
resource "aws_s3_bucket" "scripts" {
  bucket = "scripts-logs-${local.resource_suffix}"

  tags = {
    Environment = local.environment
    Project     = "csv-parquet-etl"
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==========================================
# 5. SCRIPT DE GLUE (Subido al bucket dedicado)
# ==========================================
resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "scripts/transform_job.py"
  source = "src/glue/transform_job.py"
  # Usamos md5 directo con file() para evitar problemas de resolución de path en CI/CD
  etag   = md5(file("src/glue/transform_job.py"))
}
# ==========================================
# 6. GLUE JOB (Con logs al bucket dedicado y script atado)
# ==========================================
resource "aws_glue_job" "etl_job" {
  name              = "glue-job-csv-parquet-etl-v6-276846898236"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  python_version    = "3"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/scripts/transform_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.scripts.bucket}/spark-logs/"
  }
  depends_on = [aws_s3_object.etl_script]
}

# ==========================================
# 7. SISTEMA DE ALERTAS (SNS + EventBridge)
# ==========================================
resource "aws_sns_topic" "etl_alerts" {
  name = "etl-failure-alerts-${local.resource_suffix}"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.etl_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "glue_failure_rule" {
  name        = "glue-job-failure-${local.resource_suffix}"
  description = "Captura cuando el trabajo de Glue falla o expira"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      state   = ["FAILED", "TIMEOUT"]
      jobName = [aws_glue_job.etl_job.name]
    }
  })
}

resource "aws_cloudwatch_event_target" "sns_target" {
  rule      = aws_cloudwatch_event_rule.glue_failure_rule.name
  target_id = "SendToSNS"
  arn       = aws_sns_topic.etl_alerts.arn
}

resource "aws_sns_topic_policy" "default" {
  arn    = aws_sns_topic.etl_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowEventBridgePublish"
        Effect    = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.etl_alerts.arn
      }
    ]
  })
}

# ==========================================
# 8. EVENTBRIDGE PARA DISPARAR GLUE DESDE S3
# ==========================================

# Permitir que EventBridge invoque al Glue Job
resource "aws_iam_role" "eventbridge_glue_role" {
  name = "eventbridge-glue-execution-role-${local.resource_suffix}"

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
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "eventbridge-glue-policy-${local.resource_suffix}"
  role = aws_iam_role.eventbridge_glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:StartJobRun"
        Resource = aws_glue_job.etl_job.arn
      }
    ]
  })
}

# Regla de EventBridge que escucha cuando se sube un archivo (PutObject) al bucket raw
resource "aws_cloudwatch_event_rule" "s3_to_glue_rule" {
  name        = "s3-upload-trigger-glue-${local.resource_suffix}"
  description = "Dispara el Glue Job solo ante nuevos archivos CSV en raw"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.raw.bucket]
      }
      object = {
        key = [{
          suffix = ".csv"
        }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_glue_target" {
  rule      = aws_cloudwatch_event_rule.s3_to_glue_rule.name
  target_id = "TriggerGlueJob"
  arn       = "arn:aws:glue:us-east-1:276846898236:job/${aws_glue_job.etl_job.name}"
  role_arn  = aws_iam_role.eventbridge_glue_role.arn

  depends_on = [aws_glue_job.etl_job]
}
