from datetime import date

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACION
# ============================================================

CATALOG = spark.conf.get("electrocasa.catalog")

SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


# Periodo oficial del proyecto
FECHA_INICIO = "2026-03-01"
FECHA_FIN = "2026-10-31"


# ============================================================
# 1. VENTAS NETAS POR SUCURSAL Y MES
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.ventas_netas_sucursal_mes",
    comment="Ventas, devoluciones, ventas netas, cantidad de ventas y ticket promedio por sucursal y mes."
)
def gold_ventas_netas_sucursal_mes():

    ventas = (
        spark.read.table(f"{SILVER}.ventas")
        .filter(
            F.col("fecha_venta").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .withColumn(
            "mes",
            F.trunc(F.col("fecha_venta"), "month")
        )
        .groupBy(
            "sucursal_id",
            "mes"
        )
        .agg(
            F.sum("monto_total").alias("ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    devoluciones = (
        spark.read.table(f"{SILVER}.devoluciones")
        .filter(
            F.col("fecha_devolucion").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .withColumn(
            "mes",
            F.trunc(F.col("fecha_devolucion"), "month")
        )
        .groupBy(
            "sucursal_id",
            "mes"
        )
        .agg(
            F.sum("monto_reembolso").alias("devoluciones")
        )
    )

    sucursales = (
        spark.read.table(f"{SILVER}.sucursales")
        .select(
            "sucursal_id",
            "nombre_sucursal",
            "ciudad",
            "region"
        )
    )

    resultado = (
        ventas.alias("v")
        .join(
            devoluciones.alias("d"),
            ["sucursal_id", "mes"],
            "full"
        )
        .select(
            F.col("sucursal_id"),
            F.col("mes"),

            F.coalesce(
                F.col("v.ventas"),
                F.lit(0)
            ).alias("ventas"),

            F.coalesce(
                F.col("d.devoluciones"),
                F.lit(0)
            ).alias("devoluciones"),

            F.coalesce(
                F.col("v.cantidad_ventas"),
                F.lit(0)
            ).alias("cantidad_ventas")
        )
        .withColumn(
            "ventas_netas",
            F.col("ventas") - F.col("devoluciones")
        )
        .withColumn(
            "ticket_promedio",
            F.when(
                F.col("cantidad_ventas") > 0,
                F.col("ventas") / F.col("cantidad_ventas")
            ).otherwise(F.lit(0))
        )
    )

    return (
        resultado
        .join(
            sucursales,
            "sucursal_id",
            "left"
        )
        .select(
            "sucursal_id",
            "nombre_sucursal",
            "ciudad",
            "region",
            "mes",
            "ventas",
            "devoluciones",
            "ventas_netas",
            "cantidad_ventas",
            "ticket_promedio"
        )
    )


# ============================================================
# 2A. TOP 10 PRODUCTOS POR UNIDADES VENDIDAS
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.top10_productos_unidades",
    comment="Top 10 productos con mayor cantidad de unidades vendidas."
)
def gold_top10_productos_unidades():

    ventas = (
        spark.read.table(f"{SILVER}.ventas")
        .filter(
            F.col("fecha_venta").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.sum("monto_total").alias("monto_vendido"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    catalogo = (
        spark.read.table(f"{SILVER}.catalogo")
        .select(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca"
        )
    )

    return (
        ventas
        .join(
            catalogo,
            "producto_id",
            "left"
        )
        .orderBy(
            F.col("unidades_vendidas").desc()
        )
        .limit(10)
    )


# ============================================================
# 2B. TOP 10 PRODUCTOS CON MAYOR TASA DE DEVOLUCION
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.top10_productos_tasa_devolucion",
    comment="Top 10 productos con mayor tasa de devolucion."
)
def gold_top10_productos_tasa_devolucion():

    ventas = (
        spark.read.table(f"{SILVER}.ventas")
        .filter(
            F.col("fecha_venta").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .groupBy("producto_id")
        .agg(
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    devoluciones = (
        spark.read.table(f"{SILVER}.devoluciones")
        .filter(
            F.col("fecha_devolucion").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .groupBy("producto_id")
        .agg(
            F.countDistinct("devolucion_id").alias(
                "cantidad_devoluciones"
            )
        )
    )

    catalogo = (
        spark.read.table(f"{SILVER}.catalogo")
        .select(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca"
        )
    )

    resultado = (
        ventas
        .join(
            devoluciones,
            "producto_id",
            "left"
        )
        .fillna(
            0,
            subset=["cantidad_devoluciones"]
        )
        .withColumn(
            "tasa_devolucion",
            F.when(
                F.col("cantidad_ventas") > 0,
                F.col("cantidad_devoluciones")
                / F.col("cantidad_ventas")
            ).otherwise(F.lit(0))
        )
    )

    return (
        resultado
        .join(
            catalogo,
            "producto_id",
            "left"
        )
        .orderBy(
            F.col("tasa_devolucion").desc()
        )
        .limit(10)
    )


# ============================================================
# 3. DOTACION ACTIVA POR SUCURSAL AL FIN DE CADA MES
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.dotacion_mensual_sucursal",
    comment="Cantidad de empleados activos por sucursal al ultimo dia de cada mes."
)
def gold_dotacion_mensual_sucursal():

    fechas_corte = [
        (date(2026, 3, 31),),
        (date(2026, 4, 30),),
        (date(2026, 5, 31),),
        (date(2026, 6, 30),),
        (date(2026, 7, 31),),
        (date(2026, 8, 31),),
        (date(2026, 9, 30),),
        (date(2026, 10, 31),)
    ]

    calendario = spark.createDataFrame(
        fechas_corte,
        ["fecha_corte"]
    )

    empleados = (
        spark.read.table(
            f"{SILVER}.empleados_scd2"
        )
        .select(
            "id_empleado",
            "sucursal_id",

            F.to_date(
                F.col("__START_AT.fecha_evento")
            ).alias("vigente_desde"),

            F.to_date(
                F.col("__END_AT.fecha_evento")
            ).alias("vigente_hasta")
        )
    )

    sucursales = (
        spark.read.table(f"{SILVER}.sucursales")
        .select(
            "sucursal_id",
            "nombre_sucursal",
            "ciudad",
            "region"
        )
    )

    activos = (
        calendario
        .crossJoin(empleados)
        .filter(
            (F.col("vigente_desde") <= F.col("fecha_corte"))
            &
            (
                F.col("vigente_hasta").isNull()
                |
                (F.col("fecha_corte") < F.col("vigente_hasta"))
            )
        )
        .groupBy(
            "fecha_corte",
            "sucursal_id"
        )
        .agg(
            F.countDistinct("id_empleado").alias(
                "empleados_activos"
            )
        )
    )

    return (
        activos
        .join(
            sucursales,
            "sucursal_id",
            "left"
        )
        .select(
            "fecha_corte",
            "sucursal_id",
            "nombre_sucursal",
            "ciudad",
            "region",
            "empleados_activos"
        )
    )


# ============================================================
# 4. VENTAS POR VENDEDOR, SUCURSAL HISTORICA Y MES
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.ventas_vendedor_mes",
    comment="Ventas de tienda fisica por vendedor usando la sucursal historica vigente."
)
def gold_ventas_vendedor_mes():

    ventas = (
        spark.read.table(f"{SILVER}.ventas")
        .filter(
            F.col("fecha_venta").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .filter(
            F.upper(
                F.trim(F.col("canal"))
            ).isin(
                "TIENDA",
                "TIENDA FISICA",
                "TIENDA FÍSICA",
                "TIENDA_FISICA",
                "FISICA",
                "FÍSICA"
            )
        )
        .withColumn(
            "mes",
            F.trunc("fecha_venta", "month")
        )
        .alias("v")
    )

    empleados = (
        spark.read.table(
            f"{SILVER}.empleados_scd2"
        )
        .select(
            "id_empleado",
            "nombre",
            "sucursal_id",

            F.to_date(
                F.col("__START_AT.fecha_evento")
            ).alias("vigente_desde"),

            F.to_date(
                F.col("__END_AT.fecha_evento")
            ).alias("vigente_hasta")
        )
        .alias("e")
    )

    condicion = (
        (F.col("v.vendedor_id") == F.col("e.id_empleado"))
        &
        (F.col("v.fecha_venta") >= F.col("e.vigente_desde"))
        &
        (
            F.col("e.vigente_hasta").isNull()
            |
            (F.col("v.fecha_venta") < F.col("e.vigente_hasta"))
        )
    )

    detalle = (
        ventas
        .join(
            empleados,
            condicion,
            "left"
        )
        .select(
            F.col("v.venta_id"),
            F.col("v.vendedor_id"),

            F.coalesce(
                F.col("e.nombre"),
                F.lit("SIN_ASIGNAR")
            ).alias("nombre_vendedor"),

            F.coalesce(
                F.col("e.sucursal_id"),
                F.lit("SIN_ASIGNAR")
            ).alias("sucursal_id"),

            F.col("v.mes"),
            F.col("v.monto_total")
        )
    )

    return (
        detalle
        .groupBy(
            "vendedor_id",
            "nombre_vendedor",
            "sucursal_id",
            "mes"
        )
        .agg(
            F.sum("monto_total").alias("monto_ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )


# ============================================================
# 5. PORCENTAJE DE RESENAS NEGATIVAS POR CATEGORIA Y MES
# ============================================================

@dp.materialized_view(
    name=f"{GOLD}.resenas_negativas_categoria_mes",
    comment="Porcentaje de resenas con calificacion 1 o 2 por categoria y mes."
)
def gold_resenas_negativas_categoria_mes():

    resenas = (
        spark.read.table(
            f"{SILVER}.resenas"
        )
        .filter(
            F.col("fecha_resena").between(
                F.lit(FECHA_INICIO),
                F.lit(FECHA_FIN)
            )
        )
        .withColumn(
            "mes",
            F.trunc(
                F.col("fecha_resena"),
                "month"
            )
        )
        .alias("r")
    )

    catalogo = (
        spark.read.table(
            f"{SILVER}.catalogo"
        )
        .select(
            "producto_id",
            "categoria"
        )
        .alias("c")
    )

    validas = (
        resenas
        .join(
            catalogo,
            F.col("r.producto_id")
            == F.col("c.producto_id"),
            "inner"
        )
    )

    return (
        validas
        .groupBy(
            F.col("c.categoria").alias("categoria"),
            F.col("r.mes").alias("mes")
        )
        .agg(
            F.count("*").alias("total_resenas"),

            F.sum(
                F.when(
                    F.col("r.calificacion").isin(1, 2),
                    1
                ).otherwise(0)
            ).alias("resenas_negativas")
        )
        .withColumn(
            "porcentaje_negativas",
            F.round(
                (
                    F.col("resenas_negativas")
                    / F.col("total_resenas")
                ) * 100,
                2
            )
        )
    )