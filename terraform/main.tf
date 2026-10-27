data "aws_caller_identity" "current" {}

locals {
  resource_suffix = "csv-parquet-etl-${data.aws_caller_identity.current.account_id}"
}

# ==========================================
# 1. BUCKETS S3 (Raw y State) Y S3 TABLE BUCKET (Processed - Iceberg)
# ==========================================

resource "aws_s3_bucket" "raw" {
  bucket = "raw-${local.resource_suffix}"
}

resource "aws_s3_bucket" "state" {
  bucket = "state-${local.resource_suffix}"
}

# Habilitar EventBridge en el bucket raw para notificar subidas de archivos
resource "aws_s3_bucket_notification" "raw_bucket_notification" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}

resource "aws_s3tables_table_bucket" "processed" {
  name = "processed-${local.resource_suffix}"
}

resource "aws_s3tables_namespace" "processed_namespace" {
  namespace        = "retail"
  table_bucket_arn = aws_s3tables_table_bucket.processed.arn
}

data "aws_caller_identity" "current" {}


# ==========================================
# 2. CONFIGURACION DE SNS Y ALERTAS (CloudWatch)
# ==========================================

resource "aws_sns_topic" "glue_alerts" {
  name = "${var.project_name}-alerts-topic"
}

resource "aws_sns_topic_subscription" "email_sub" {
  topic_arn = aws_sns_topic.glue_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "glue_fail_alarm" {
  alarm_name          = "${var.project_name}-job-failed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "FailedExecutions"
  namespace           = "AWS/Glue"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarma cuando el job de AWS Glue falla"
  alarm_actions       = [aws_sns_topic.glue_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.etl_job.name
  }
}


# ==========================================
# 3. ROLES Y PERMISOS IAM (Glue y EventBridge)
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
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_custom_policy" {
  name = "${var.project_name}-glue-custom-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.state.arn,
          "${aws_s3_bucket.state.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3tables:GetTableData",
          "s3tables:PutTableData",
          "s3tables:GetTableMetadataLocation",
          "s3tables:ListTables",
          "s3tables:GetTable"
        ]
        Resource = "${aws_s3tables_table_bucket.processed.arn}/*"
      }
    ]
  })
}

# Rol para permitir que EventBridge dispare el Job de Glue
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
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "eventbridge-glue-start-job-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ]
        Resource = "*"  # O el ARN específico de tu Glue Job
      }
    ]
  })
}


# ==========================================
# 4. DISPARADOR AUTOMATICO (EVENTBRIDGE RULE)
# ==========================================

resource "aws_cloudwatch_event_rule" "s3_upload_rule" {
  name        = "${var.project_name}-s3-upload-rule"
  description = "Dispara el job de Glue cuando se sube un archivo al bucket raw"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.raw.id]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_target" {
  rule      = aws_cloudwatch_event_rule.s3_upload_rule.name
  target_id = "TriggerGlueJobFromS3"
  arn       = aws_glue_job.etl_job.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  # Forzar que el Job y el Rol IAM estén listos antes de crear el Target
  depends_on = [
    aws_glue_job.etl_job,
    aws_iam_role_policy.eventbridge_glue_policy
  ]
}


# ==========================================
# 5. RECURSO DE AWS GLUE JOB
# ==========================================

resource "aws_glue_job" "etl_job" {
  name              = "${var.project_name}-job"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.state.id}/scripts/transform_job.py"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.state.id}/temporary/"
  }
}