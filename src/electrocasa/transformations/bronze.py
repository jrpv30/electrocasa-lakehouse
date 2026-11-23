from pyspark import pipelines as dp

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType
)


# ============================================================
# CONFIGURACION
# ============================================================

CATALOG = spark.conf.get("electrocasa.catalog")

LANDING_PATH = f"/Volumes/{CATALOG}/default/vol_landing"


# ============================================================
# SCHEMA EXPLICITO - VENTAS
# ============================================================

ventas_schema = StructType([
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("vendedor_id", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("monto_total", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("fecha_venta", StringType(), True),
    StructField("canal", StringType(), True),
])


# ============================================================
# BRONZE - VENTAS
# ============================================================

@dp.table(
    name="bronze_ventas",
    comment="Capa Bronze de ventas. Datos crudos provenientes de archivos CSV."
)
def bronze_ventas():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(ventas_schema)
        .load(f"{LANDING_PATH}/ventas/")
        .selectExpr(
            "*",
            "current_timestamp() AS _ingestion_timestamp",
            "_metadata.file_name AS _source_file"
        )
    )


# ============================================================
# SCHEMA EXPLICITO - EMPLEADOS
# ============================================================

empleados_schema = StructType([
    StructField("id_empleado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("dni", StringType(), True),
    StructField("email", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("fecha_evento", StringType(), True),
    StructField("fecha_registro", StringType(), True),
])


# ============================================================
# BRONZE - EMPLEADOS
# ============================================================

@dp.table(
    name="bronze_empleados",
    comment="Capa Bronze de empleados. Eventos crudos provenientes de archivos CSV."
)
def bronze_empleados():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(empleados_schema)
        .load(f"{LANDING_PATH}/empleados/")
        .selectExpr(
            "*",
            "current_timestamp() AS _ingestion_timestamp",
            "_metadata.file_name AS _source_file"
        )
    )


# ============================================================
# SCHEMA ANIDADO - RESPUESTAS DE RESENAS
# ============================================================

respuesta_schema = StructType([
    StructField("autor", StringType(), True),
    StructField("texto", StringType(), True),
    StructField("fecha", StringType(), True),
])


# ============================================================
# SCHEMA EXPLICITO - RESENAS
# ============================================================

resenas_schema = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("fecha_resena", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", ArrayType(respuesta_schema), True),
    StructField("metadata", StringType(), True),
])


# ============================================================
# BRONZE - RESENAS
# ============================================================

@dp.table(
    name="bronze_resenas",
    comment="Capa Bronze de resenas. Datos JSON Lines con estructuras anidadas."
)
def bronze_resenas():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(resenas_schema)
        .load(f"{LANDING_PATH}/resenas/")
        .selectExpr(
            "*",
            "current_timestamp() AS _ingestion_timestamp",
            "_metadata.file_name AS _source_file"
        )
    )