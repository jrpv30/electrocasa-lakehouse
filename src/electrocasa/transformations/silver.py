dp.create_streaming_table(
    name=f"{SILVER}.empleados_scd2",
    comment="Historico SCD Type 2 de empleados con masking de DNI y salario.",
    schema=f"""
        id_empleado STRING,
        nombre STRING,
        dni STRING MASK {CATALOG}.default.mask_dni,
        email STRING,
        salario DECIMAL(18,2) MASK {CATALOG}.default.mask_salario,
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        fecha_evento TIMESTAMP,
        fecha_registro TIMESTAMP,
        __START_AT STRUCT<
            fecha_evento: TIMESTAMP,
            fecha_registro: TIMESTAMP
        >,
        __END_AT STRUCT<
            fecha_evento: TIMESTAMP,
            fecha_registro: TIMESTAMP
        >
    """
)