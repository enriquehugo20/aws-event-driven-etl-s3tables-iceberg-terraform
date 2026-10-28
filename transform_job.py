import sys
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Capturar argumentos necesarios del Job definidos en Terraform
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_BUCKET', 'PROCESSED_BUCKET', 'STATE_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

job_name = args['JOB_NAME']
raw_bucket = args['RAW_BUCKET']
processed_bucket = args['PROCESSED_BUCKET']
state_bucket = args['STATE_BUCKET']

# Rutas de S3
# Nota: Si EventBridge pasa un archivo especifico, se puede extraer de los argumentos extra, 
# por defecto leemos el contenido general del bucket raw para este patron ETL.
input_path = f"s3://{raw_bucket}/"
output_path = f"s3://{processed_bucket}/"
state_path = f"s3://{state_bucket}/states/"

file_name = "dynamic_upload.csv"
current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

try:
    print(f"Iniciando lectura del archivo CSV desde: {input_path}")
    
    # 1. Leer el archivo CSV con cabecera e inferencia de esquema
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # 2. Transformacion sencilla y limpieza
    df_transformed = df.withColumn("product_category", F.upper(F.trim(F.col("product_category")))) \
                       .withColumn("store_location", F.trim(F.col("store_location")))
    
    # 3. Agregar columnas extra solicitadas (processed_datetime para filtrar despues)
    df_final = df_transformed.withColumn("processed_datetime", F.lit(current_timestamp)) \
                             .withColumn("etl_status", F.lit("SUCCESS"))
    
    print("Transformacion y limpieza aplicadas exitosamente.")
    
    # 4. Guardar en formato Parquet en el bucket processed (S3 Table Bucket / Iceberg compatible)
    df_final.write.mode("append").parquet(output_path)
    print(f"Datos guardados en formato Parquet en: {output_path}")
    
    # 5. Generar y guardar archivo de estado (JSON) en el bucket state
    status_data = [{
        "job_name": job_name,
        "input_file": file_name,
        "status": "SUCCESS",
        "rows_processed": df_final.count(),
        "processed_timestamp": current_timestamp,
        "message": "Transformacion a parquet completada y guardada correctamente"
    }]
    
    df_status = spark.createDataFrame(status_data)
    df_status.write.mode("append").json(state_path)
    print(f"Archivo de estado guardado en: {state_path}")

except Exception as e:
    error_message = str(e)
    print(f"Error critico en el Job: {error_message}")
    
    # Registrar estado de fallo en el bucket state
    fail_data = [{
        "job_name": job_name,
        "input_file": file_name,
        "status": "FAILED",
        "processed_timestamp": current_timestamp,
        "error_message": error_message
    }]
    
    try:
        spark.createDataFrame(fail_data).write.mode("append").json(state_path)
    except Exception:
        pass
        
    raise e

job.commit()