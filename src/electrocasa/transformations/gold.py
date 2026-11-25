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
            ) == "TIENDA_FISICA"
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