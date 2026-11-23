from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURACION
# ============================================================

CATALOG = spark.conf.get("electrocasa.catalog")

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def normalizar_decimal(columna):
    """
    Normaliza montos que pueden llegar como:
    1234.50
    1,234.50
    1.234,50
    S/ 1.234,50
    """
    limpio = F.regexp_replace(
        F.trim(columna.cast("string")),
        r"[^0-9,.\-]",
        ""
    )

    return (
        F.when(
            limpio.rlike(r"^-?\d{1,3}(\.\d{3})+,\d+$"),
            F.regexp_replace(
                F.regexp_replace(limpio, r"\.", ""),
                ",",
                "."
            )
        )
        .when(
            limpio.rlike(r"^-?\d+,\d+$"),
            F.regexp_replace(limpio, ",", ".")
        )
        .otherwise(
            F.regexp_replace(limpio, ",", "")
        )
        .cast("decimal(18,2)")
    )


def normalizar_fecha(columna):
    return F.coalesce(
        F.to_date(columna, "yyyy-MM-dd"),
        F.to_date(columna, "dd/MM/yyyy"),
        F.to_date(columna, "MM/dd/yyyy"),
        F.to_date(columna, "yyyy/MM/dd"),
        F.to_date(columna)
    )


def normalizar_timestamp(columna):
    return F.coalesce(
        F.to_timestamp(columna, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(columna, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(columna, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(columna)
    )


# ============================================================
# SILVER - VENTAS BASE
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.ventas",
    comment="Ventas limpias, normalizadas y deduplicadas."
)
@dp.expect_or_drop(
    "venta_id_obligatorio",
    "venta_id IS NOT NULL"
)
@dp.expect_or_drop(
    "monto_total_positivo",
    "monto_total > 0"
)
def silver_ventas():

    df = spark.read.table(f"{BRONZE}.bronze_ventas")

    df = (
        df
        .withColumn(
            "cantidad",
            F.col("cantidad").cast("int")
        )
        .withColumn(
            "monto_total",
            normalizar_decimal(F.col("monto_total"))
        )
        .withColumn(
            "fecha_venta",
            normalizar_fecha(F.col("fecha_venta"))
        )
        .withColumn(
            "metodo_pago",
            F.when(
                F.lower(F.trim("metodo_pago")).isin(
                    "efectivo", "efv"
                ),
                "EFECTIVO"
            )
            .when(
                F.lower(F.trim("metodo_pago")).isin(
                    "tarjeta", "tc"
                ),
                "TARJETA"
            )
            .when(
                F.lower(F.trim("metodo_pago")).isin(
                    "transferencia"
                ),
                "TRANSFERENCIA"
            )
            .when(
                F.lower(F.trim("metodo_pago")) == "yape",
                "YAPE"
            )
            .when(
                F.lower(F.trim("metodo_pago")) == "plin",
                "PLIN"
            )
            .otherwise(
                F.upper(F.trim("metodo_pago"))
            )
        )
        .withColumn(
            "canal",
            F.upper(F.trim("canal"))
        )
    )

    ventana = Window.partitionBy("venta_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# CUARENTENA - VENTAS
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_ventas",
    comment="Registros de ventas rechazados por reglas críticas."
)
def cuarentena_ventas():

    df = spark.read.table(f"{BRONZE}.bronze_ventas")

    df = (
        df
        .withColumn(
            "_monto_normalizado",
            normalizar_decimal(F.col("monto_total"))
        )
        .withColumn(
            "_fecha_normalizada",
            normalizar_fecha(F.col("fecha_venta"))
        )
    )

    return (
        df
        .filter(
            F.col("venta_id").isNull()
            | (F.col("_monto_normalizado").isNull())
            | (F.col("_monto_normalizado") <= 0)
            | F.col("_fecha_normalizada").isNull()
        )
        .withColumn(
            "_fuente",
            F.lit("ventas")
        )
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("venta_id").isNull(),
                F.lit("venta_id_obligatorio")
            )
            .when(
                F.col("_monto_normalizado").isNull(),
                F.lit("monto_total_no_numerico")
            )
            .when(
                F.col("_monto_normalizado") <= 0,
                F.lit("monto_total_positivo")
            )
            .otherwise(
                F.lit("fecha_venta_invalida")
            )
        )
    )


# ============================================================
# SILVER - CATALOGO
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.catalogo",
    comment="Ultimo registro valido por producto."
)
@dp.expect_or_drop(
    "precio_lista_positivo",
    "precio_lista > 0"
)
def silver_catalogo():

    df = spark.read.table(f"{BRONZE}.catalogo")

    df = (
        df
        .withColumn(
            "precio_lista",
            normalizar_decimal(F.col("precio_lista"))
        )
        .withColumn(
            "categoria",
            F.upper(F.trim("categoria"))
        )
        .withColumn(
            "marca",
            F.upper(F.trim("marca"))
        )
        .withColumn(
            "activo",
            F.when(
                F.lower(F.trim("activo")).isin(
                    "true", "1", "si", "sí", "activo"
                ),
                F.lit(True)
            )
            .otherwise(F.lit(False))
        )
    )

    ventana = Window.partitionBy("producto_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# CUARENTENA - CATALOGO
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_catalogo"
)
def cuarentena_catalogo():

    df = spark.read.table(f"{BRONZE}.catalogo")

    precio = normalizar_decimal(F.col("precio_lista"))

    return (
        df
        .withColumn("_precio_normalizado", precio)
        .filter(
            F.col("producto_id").isNull()
            | F.col("_precio_normalizado").isNull()
            | (F.col("_precio_normalizado") <= 0)
        )
        .withColumn("_fuente", F.lit("catalogo"))
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("producto_id").isNull(),
                F.lit("producto_id_obligatorio")
            )
            .otherwise(
                F.lit("precio_lista_invalido")
            )
        )
    )


# ============================================================
# SILVER - DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.devoluciones",
    comment="Devoluciones normalizadas y deduplicadas."
)
@dp.expect_or_drop(
    "reembolso_no_negativo",
    "monto_reembolso >= 0"
)
def silver_devoluciones():

    df = spark.read.table(f"{BRONZE}.devoluciones")

    df = (
        df
        .withColumn(
            "fecha_devolucion",
            normalizar_fecha(F.col("fecha_devolucion"))
        )
        .withColumn(
            "monto_reembolso",
            normalizar_decimal(F.col("monto_reembolso"))
        )
        .withColumn(
            "motivo",
            F.upper(F.trim("motivo"))
        )
    )

    ventana = Window.partitionBy("devolucion_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# CUARENTENA - DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_devoluciones"
)
def cuarentena_devoluciones():

    df = spark.read.table(f"{BRONZE}.devoluciones")

    monto = normalizar_decimal(F.col("monto_reembolso"))

    return (
        df
        .withColumn("_monto_normalizado", monto)
        .filter(
            F.col("devolucion_id").isNull()
            | F.col("venta_id").isNull()
            | F.col("_monto_normalizado").isNull()
            | (F.col("_monto_normalizado") < 0)
        )
        .withColumn("_fuente", F.lit("devoluciones"))
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("devolucion_id").isNull(),
                F.lit("devolucion_id_obligatorio")
            )
            .when(
                F.col("venta_id").isNull(),
                F.lit("venta_id_obligatorio")
            )
            .otherwise(
                F.lit("monto_reembolso_invalido")
            )
        )
    )


# ============================================================
# SILVER - SUCURSALES
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.sucursales"
)
def silver_sucursales():

    return (
        spark.read.table(f"{BRONZE}.sucursales")
        .select(
            "sucursal_id",
            "nombre_sucursal",
            F.initcap(F.trim("distrito")).alias("distrito"),
            F.initcap(F.trim("ciudad")).alias("ciudad"),
            F.upper(F.trim("region")).alias("region"),
            "_ingestion_timestamp",
            "_source_file"
        )
        .dropDuplicates(["sucursal_id"])
    )


# ============================================================
# SILVER - RESENAS
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.resenas",
    comment="Resenas validas y deduplicadas."
)
@dp.expect_or_drop(
    "calificacion_valida",
    "calificacion BETWEEN 1 AND 5"
)
def silver_resenas():

    df = spark.read.table(f"{BRONZE}.bronze_resenas")

    df = (
        df
        .withColumn(
            "fecha_resena",
            normalizar_fecha(F.col("fecha_resena"))
        )
        .withColumn(
            "calificacion",
            F.col("calificacion").cast("int")
        )
    )

    ventana = Window.partitionBy("resena_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# CUARENTENA - RESENAS
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_resenas"
)
def cuarentena_resenas():

    df = (
        spark.read.table(f"{BRONZE}.bronze_resenas")
        .withColumn(
            "_calificacion_normalizada",
            F.col("calificacion").cast("int")
        )
    )

    return (
        df
        .filter(
            F.col("resena_id").isNull()
            | F.col("_calificacion_normalizada").isNull()
            | ~F.col("_calificacion_normalizada").between(1, 5)
        )
        .withColumn("_fuente", F.lit("resenas"))
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("resena_id").isNull(),
                F.lit("resena_id_obligatorio")
            )
            .otherwise(
                F.lit("calificacion_fuera_de_rango")
            )
        )
    )


# ============================================================
# EMPLEADOS - EVENTOS NORMALIZADOS
# ============================================================

@dp.view(
    name="empleados_eventos_limpios"
)
def empleados_eventos_limpios():

    return (
        spark.read.table(f"{BRONZE}.bronze_empleados")
        .filter(
            F.col("id_empleado").isNotNull()
            & F.col("fecha_evento").isNotNull()
        )
        .withColumn(
            "salario",
            normalizar_decimal(F.col("salario"))
        )
        .withColumn(
            "fecha_evento",
            normalizar_timestamp(F.col("fecha_evento"))
        )
        .withColumn(
            "fecha_registro",
            normalizar_timestamp(F.col("fecha_registro"))
        )
        .withColumn(
            "tipo_evento",
            F.when(
                F.lower(F.trim("tipo_evento")).isin(
                    "alta", "ingreso"
                ),
                "ALTA"
            )
            .when(
                F.lower(F.trim("tipo_evento")).isin(
                    "transferencia", "traslado"
                ),
                "TRANSFERENCIA"
            )
            .when(
                F.lower(F.trim("tipo_evento")).isin(
                    "cambio_salario",
                    "cambio salario",
                    "aumento"
                ),
                "CAMBIO_SALARIO"
            )
            .when(
                F.lower(F.trim("tipo_evento")).isin(
                    "baja", "cese"
                ),
                "BAJA"
            )
            .otherwise(
                F.upper(F.trim("tipo_evento"))
            )
        )
    )


# ============================================================
# EMPLEADOS - SCD TYPE 2
# ============================================================
#
# sequence_by usa columnas DEL ORIGEN:
# fecha_evento + fecha_registro.
#
# No usamos current_timestamp(), porque perderiamos el orden
# de eventos tardios o eventos del mismo dia.
# ============================================================

dp.create_streaming_table(
    name=f"{SILVER}.empleados_scd2",
    comment="Historico de empleados implementado como SCD Type 2."
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.empleados_scd2",
    source="empleados_eventos_limpios",
    keys=["id_empleado"],
    sequence_by=F.struct(
        F.col("fecha_evento"),
        F.col("fecha_registro")
    ),
    apply_as_deletes=F.expr(
        "tipo_evento = 'BAJA'"
    ),
    except_column_list=[
        "_ingestion_timestamp",
        "_source_file"
    ],
    stored_as_scd_type=2
)


# ============================================================
# CUARENTENA - EMPLEADOS
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_empleados"
)
def cuarentena_empleados():

    df = spark.read.table(f"{BRONZE}.bronze_empleados")

    return (
        df
        .filter(
            F.col("id_empleado").isNull()
            | F.col("fecha_evento").isNull()
        )
        .withColumn("_fuente", F.lit("empleados"))
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("id_empleado").isNull(),
                F.lit("id_empleado_obligatorio")
            )
            .otherwise(
                F.lit("fecha_evento_obligatoria")
            )
        )
    )