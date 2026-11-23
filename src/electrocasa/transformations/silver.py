# ============================================================
# EMPLEADOS - EVENTOS NORMALIZADOS
# ============================================================

@dp.view(
    name="empleados_eventos_limpios"
)
def empleados_eventos_limpios():

    return (
        spark.readStream.table(f"{BRONZE}.bronze_empleados")
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