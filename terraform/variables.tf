variable "aws_region" {
  type        = string
  default     = "us-east-1" # Puedes cambiarla a tu region preferida
  description = "Region de AWS para desplegar los recursos"
}

variable "project_name" {
  type        = string
  default     = "csv-parquet-etl"
  description = "Prefijo para nombrar los recursos de forma unica"
}

variable "alert_email" {
  type        = string
  default     = "enriquehugo2000@gmail.com" # Cambia esto por tu correo real
  description = "Correo electronico para recibir las alertas de fallo de Glue"
}