from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURACION
# ============================================================

CATALOG = spark.conf.get("electrocasa.catalog")

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


# ============================================================
# FUNCIONES DE NORMALIZACION
# ============================================================

def texto_normalizado(columna):
    """
    Normaliza texto:
    - trim
    - minusculas
    - elimina tildes comunes
    - espacios y guiones pasan a _
    """

    return F.lower(
        F.regexp_replace(
            F.translate(
                F.trim(columna),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            ),
            r"[\s\-]+",
            "_"
        )
    )


def normalizar_estado(columna):

    valor = texto_normalizado(columna)

    return (
        F.when(
            valor == "pendiente",
            F.lit("pendiente")
        )
        .when(
            valor == "entregado",
            F.lit("entregado")
        )
        .when(
            valor.isin(
                "en_transito",
                "en_camino"
            ),
            F.lit("en_transito")
        )
        .when(
            valor == "devuelto",
            F.lit("devuelto")
        )
        .otherwise(
            F.lit(None)
        )
    )


def normalizar_courier(columna):

    valor = F.lower(
        F.regexp_replace(
            F.trim(columna),
            r"\s+",
            " "
        )
    )

    return (
        F.when(
            valor == "servientrega",
            F.lit("SERVIENTREGA")
        )
        .when(
            valor == "shalom",
            F.lit("SHALOM")
        )
        .when(
            valor.isin(
                "olva",
                "olva courier"
            ),
            F.lit("OLVA COURIER")
        )
        .otherwise(
            F.lit(None)
        )
    )


# ============================================================
# SILVER - TRACKING ENVIOS
# ============================================================
#
# Se normalizan:
# - estados
# - couriers
# - timestamps
#
# Se eliminan mensajes reprocesados.
# Un reproceso se considera el mismo:
#
# tracking_id + estado + fecha_modificacion
#
# aunque tenga un evento_id diferente.
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.tracking_envios",
    comment="Tracking de envios normalizado, validado y deduplicado."
)
@dp.expect_or_drop(
    "evento_id_obligatorio",
    "evento_id IS NOT NULL"
)
@dp.expect_or_drop(
    "tracking_id_obligatorio",
    "tracking_id IS NOT NULL"
)
@dp.expect_or_drop(
    "fecha_modificacion_obligatoria",
    "fecha_modificacion IS NOT NULL"
)
@dp.expect_or_drop(
    "estado_entrega_valido",
    """
    estado_entrega IN (
        'pendiente',
        'en_transito',
        'entregado',
        'devuelto'
    )
    """
)
@dp.expect_or_drop(
    "courier_valido",
    """
    courier IN (
        'SERVIENTREGA',
        'SHALOM',
        'OLVA COURIER'
    )
    """
)
def silver_tracking_envios():

    df = (
        spark.read.table(
            f"{BRONZE}.tracking_envios"
        )
        .withColumn(
            "estado_entrega",
            normalizar_estado(
                F.col("estado_entrega")
            )
        )
        .withColumn(
            "courier",
            normalizar_courier(
                F.col("courier")
            )
        )
        .withColumn(
            "fecha_actualizacion",
            F.col("fecha_actualizacion").cast("timestamp")
        )
        .withColumn(
            "fecha_modificacion",
            F.col("fecha_modificacion").cast("timestamp")
        )
    )

    # --------------------------------------------------------
    # ELIMINAR MENSAJES REPROCESADOS
    # --------------------------------------------------------

    ventana = (
        Window
        .partitionBy(
            "tracking_id",
            "estado_entrega",
            "fecha_modificacion"
        )
        .orderBy(
            F.col("evento_id").asc()
        )
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana)
        )
        .filter(
            F.col("_rn") == 1
        )
        .drop("_rn")
        .select(
            "evento_id",
            "tracking_id",
            "venta_id",
            "courier",
            "estado_entrega",
            "sucursal_origen",
            "fecha_actualizacion",
            "fecha_modificacion"
        )
    )


# ============================================================
# CUARENTENA - TRACKING
# ============================================================

@dp.materialized_view(
    name=f"{SILVER}.cuarentena_tracking",
    comment="Eventos de tracking que no cumplen reglas criticas."
)
def cuarentena_tracking():

    df = (
        spark.read.table(
            f"{BRONZE}.tracking_envios"
        )
        .withColumn(
            "_estado_normalizado",
            normalizar_estado(
                F.col("estado_entrega")
            )
        )
        .withColumn(
            "_courier_normalizado",
            normalizar_courier(
                F.col("courier")
            )
        )
        .withColumn(
            "_fecha_modificacion",
            F.col("fecha_modificacion").cast("timestamp")
        )
    )

    return (
        df
        .filter(
            F.col("evento_id").isNull()
            | F.col("tracking_id").isNull()
            | F.col("_fecha_modificacion").isNull()
            | F.col("_estado_normalizado").isNull()
            | F.col("_courier_normalizado").isNull()
        )
        .withColumn(
            "_fuente",
            F.lit("tracking_envios")
        )
        .withColumn(
            "_regla_fallida",
            F.when(
                F.col("evento_id").isNull(),
                F.lit("evento_id_obligatorio")
            )
            .when(
                F.col("tracking_id").isNull(),
                F.lit("tracking_id_obligatorio")
            )
            .when(
                F.col("_fecha_modificacion").isNull(),
                F.lit("fecha_modificacion_invalida")
            )
            .when(
                F.col("_estado_normalizado").isNull(),
                F.lit("estado_entrega_invalido")
            )
            .otherwise(
                F.lit("courier_invalido")
            )
        )
    )


# ============================================================
# GOLD - DESPACHOS POR COURIER
# ============================================================
#
# Calcula:
#
# 1. Horas promedio entre:
#    primer pendiente -> primer entregado
#
# 2. Porcentaje de envios devueltos:
#
#    envios con devuelto
#    -------------------------
#    envios con estado final
#
# Estado final:
# entregado o devuelto
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.despachos_courier",
    comment="Indicadores de tiempo de entrega y devoluciones por courier."
)
def gold_despachos_courier():

    tracking = (
        spark.read.table(
            f"{SILVER}.tracking_envios"
        )
    )

    # --------------------------------------------------------
    # RESUMEN POR ENVIO
    # --------------------------------------------------------

    envios = (
        tracking
        .groupBy(
            "tracking_id",
            "courier"
        )
        .agg(

            # Primer pendiente
            F.min(
                F.when(
                    F.col("estado_entrega") == "pendiente",
                    F.col("fecha_modificacion")
                )
            ).alias(
                "primer_pendiente"
            ),

            # Primer entregado
            F.min(
                F.when(
                    F.col("estado_entrega") == "entregado",
                    F.col("fecha_modificacion")
                )
            ).alias(
                "primer_entregado"
            ),

            # El envio tuvo alguna devolucion
            F.max(
                F.when(
                    F.col("estado_entrega") == "devuelto",
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "fue_devuelto"
            ),

            # Llego a un estado final
            F.max(
                F.when(
                    F.col("estado_entrega").isin(
                        "entregado",
                        "devuelto"
                    ),
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "llego_estado_final"
            )
        )
    )

    # --------------------------------------------------------
    # TIEMPO ENTRE PENDIENTE Y ENTREGADO
    # --------------------------------------------------------

    envios = (
        envios
        .withColumn(
            "horas_entrega",
            F.when(
                F.col("primer_pendiente").isNotNull()
                & F.col("primer_entregado").isNotNull(),

                (
                    F.col("primer_entregado").cast("long")
                    -
                    F.col("primer_pendiente").cast("long")
                ) / F.lit(3600.0)
            )
        )
    )

    # --------------------------------------------------------
    # INDICADORES POR COURIER
    # --------------------------------------------------------

    resultado = (
        envios
        .groupBy(
            "courier"
        )
        .agg(

            F.round(
                F.avg("horas_entrega"),
                2
            ).alias(
                "tiempo_promedio_horas"
            ),

            F.countDistinct(
                F.when(
                    F.col("primer_entregado").isNotNull(),
                    F.col("tracking_id")
                )
            ).alias(
                "envios_entregados"
            ),

            F.sum(
                "fue_devuelto"
            ).alias(
                "envios_devueltos"
            ),

            F.sum(
                "llego_estado_final"
            ).alias(
                "envios_estado_final"
            )
        )
        .withColumn(
            "porcentaje_envios_devueltos",
            F.when(
                F.col("envios_estado_final") > 0,

                F.round(
                    (
                        F.col("envios_devueltos")
                        /
                        F.col("envios_estado_final")
                    ) * 100,
                    2
                )
            ).otherwise(
                F.lit(0)
            )
        )
        .orderBy(
            "courier"
        )
    )

    return resultado