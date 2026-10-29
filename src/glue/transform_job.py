
import sys
import json
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# --------------------------------------------------
# 1. Argumentos y contexto de Glue
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "PROCESSED_BUCKET",
        "STATE_BUCKET",
    ],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

job_name = args["JOB_NAME"]
raw_bucket = args["RAW_BUCKET"]
table_bucket = args["PROCESSED_BUCKET"]
state_bucket = args["STATE_BUCKET"]

# Catálogo y tabla configurados en los argumentos de Glue.
target_table = "s3tables.sales.sales"

print("Catálogo actual:")
spark.sql("SHOW CATALOGS").show(truncate=False)

print("Namespaces visibles en s3tables:")
spark.sql("SHOW NAMESPACES IN s3tables").show(truncate=False)

s3 = boto3.client("s3")

# Una clave diferente por ejecución para no sobrescribir los logs.
run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)
timestamp_text = started_at.isoformat()

state_key = (
    f"states/{job_name}/"
    f"{started_at.strftime('%Y/%m/%d')}/"
    f"{run_id}.json"
)


# --------------------------------------------------
# 2. Guardar el estado directamente en S3
# --------------------------------------------------

def save_status(status, rows_processed=0, message=""):
    record = {
        "job_name": job_name,
        "run_id": run_id,
        "status": status,
        "input_bucket": raw_bucket,
        "input_pattern": "/*.csv",
        "table_bucket": table_bucket,
        "target_table": target_table,
        "rows_processed": rows_processed,
        "processed_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "message": message,
    }

    s3.put_object(
        Bucket=state_bucket,
        Key=state_key,
        Body=json.dumps(record, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    print(
        f"Log guardado: s3://{state_bucket}/{state_key}"
    )


# --------------------------------------------------
# 3. Leer, validar y transformar el CSV
# --------------------------------------------------

try:
    input_path = f"s3://{raw_bucket}/*.csv"

    print(f"Leyendo archivos CSV: {input_path}")
    print(f"Tabla de destino: {target_table}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(input_path)
    )

    # Elimina espacios accidentales en los encabezados.
    df = df.toDF(*[column.strip() for column in df.columns])

    print("Columnas detectadas:", df.columns)
    df.printSchema()
    df.show(5, truncate=False)

    required_columns = [
        "transaction_id",
        "customer_id",
        "product_category",
        "amount",
        "store_location",
        "transaction_date",
    ]

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Faltan columnas requeridas: {missing}. "
            f"Columnas detectadas: {df.columns}"
        )

    # Normaliza tipos y texto.
    df_transformed = (
        df
        .withColumn(
            "transaction_id",
            F.trim(F.col("transaction_id").cast("string")),
        )
        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id").cast("string")),
        )
        .withColumn(
            "product_category",
            F.upper(F.trim(F.col("product_category").cast("string"))),
        )
        .withColumn(
            "amount",
            F.col("amount").cast("double"),
        )
        .withColumn(
            "store_location",
            F.trim(F.col("store_location").cast("string")),
        )
        .withColumn(
            "transaction_date",
            F.to_date(F.col("transaction_date")),
        )
    )

    # Valida que las conversiones de tipo no hayan fallado.
    invalid_rows = df_transformed.filter(
        F.col("transaction_id").isNull()
        | F.col("transaction_date").isNull()
        | F.col("amount").isNull()
    ).count()

    if invalid_rows > 0:
        raise ValueError(
            f"Se encontraron {invalid_rows} filas con "
            "transaction_id, transaction_date o amount inválidos."
        )

    current_timestamp = datetime.now(timezone.utc)

    df_final = (
        df_transformed
        .withColumn(
            "processed_datetime",
            F.lit(current_timestamp).cast("timestamp"),
        )
        .withColumn(
            "etl_status",
            F.lit("SUCCESS"),
        )
        .select(
            "transaction_id",
            "customer_id",
            "product_category",
            "amount",
            "store_location",
            "transaction_date",
            "processed_datetime",
            "etl_status",
        )
    )

    rows_processed = df_final.count()

    print(f"Filas que se van a escribir: {rows_processed}")
    df_final.show(5, truncate=False)

    if rows_processed == 0:
        raise ValueError(
            "No se encontraron filas para procesar."
        )

    # --------------------------------------------------
    # 4. Escribir en la tabla Iceberg existente
    # --------------------------------------------------

    print(f"Escribiendo en la tabla Iceberg: {target_table}")

    (
        df_final.writeTo(target_table)
        .append()
    )

    print("Escritura en Iceberg completada.")

    # --------------------------------------------------
    # 5. Registrar éxito
    # --------------------------------------------------

    save_status(
        status="SUCCESS",
        rows_processed=rows_processed,
        message="CSV transformado y escrito en la tabla Iceberg.",
    )

    job.commit()
    print("ETL finalizado correctamente.")

except Exception as e:
    error_message = str(e)

    print(f"ERROR EN EL ETL: {error_message}")

    try:
        save_status(
            status="FAILED",
            rows_processed=0,
            message=error_message,
        )
    except Exception as log_error:
        print(
            "No fue posible guardar el log de error en S3: "
            f"{log_error}"
        )

    # Mantiene el estado FAILED en AWS Glue.
    raise
