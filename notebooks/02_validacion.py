# Databricks notebook source

# ============================================================
# ELECTROCASA - VALIDACION FINAL DEL PIPELINE
# ============================================================

dbutils.widgets.text("catalog", "electrocasa_dev")

CATALOG = dbutils.widgets.get("catalog")

print("=" * 60)
print("ELECTROCASA - VALIDACION FINAL")
print(f"Catalogo: {CATALOG}")
print("=" * 60)


# ============================================================
# TABLAS SILVER
# ============================================================

silver_tables = [
    "ventas",
    "catalogo",
    "devoluciones",
    "sucursales",
    "resenas",
    "empleados_scd2",
    "tracking_envios"
]

print("\n")
print("=" * 60)
print("CONTEOS SILVER")
print("=" * 60)

for tabla in silver_tables:

    nombre = f"{CATALOG}.silver.{tabla}"

    cantidad = spark.sql(
        f"SELECT COUNT(*) AS cantidad FROM {nombre}"
    ).collect()[0]["cantidad"]

    print(f"{tabla}: {cantidad} registros")


# ============================================================
# RECHAZADOS
# ============================================================

cuarentenas = {
    "ventas": "cuarentena_ventas",
    "catalogo": "cuarentena_catalogo",
    "devoluciones": "cuarentena_devoluciones",
    "resenas": "cuarentena_resenas",
    "empleados": "cuarentena_empleados",
    "tracking": "cuarentena_tracking"
}

print("\n")
print("=" * 60)
print("REGISTROS RECHAZADOS")
print("=" * 60)

for fuente, tabla in cuarentenas.items():

    nombre = f"{CATALOG}.silver.{tabla}"

    cantidad = spark.sql(
        f"SELECT COUNT(*) AS cantidad FROM {nombre}"
    ).collect()[0]["cantidad"]

    print(f"{fuente}: {cantidad} rechazados")


# ============================================================
# TABLAS GOLD
# ============================================================

gold_tables = [
    "ventas_netas_sucursal_mes",
    "top10_productos_unidades",
    "top10_productos_tasa_devolucion",
    "dotacion_mensual_sucursal",
    "ventas_vendedor_mes",
    "resenas_negativas_categoria_mes",
    "despachos_courier"
]

print("\n")
print("=" * 60)
print("CONTEOS GOLD")
print("=" * 60)

errores = []

for tabla in gold_tables:

    nombre = f"{CATALOG}.gold.{tabla}"

    cantidad = spark.sql(
        f"SELECT COUNT(*) AS cantidad FROM {nombre}"
    ).collect()[0]["cantidad"]

    print(f"{tabla}: {cantidad} registros")

    if cantidad == 0:
        errores.append(
            f"La tabla {nombre} esta vacia."
        )


# ============================================================
# VALIDACION ADICIONAL TRACKING
# ============================================================

tracking_bronze = spark.sql(
    f"""
    SELECT COUNT(*) AS cantidad
    FROM {CATALOG}.bronze.tracking_envios
    """
).collect()[0]["cantidad"]

tracking_silver = spark.sql(
    f"""
    SELECT COUNT(*) AS cantidad
    FROM {CATALOG}.silver.tracking_envios
    """
).collect()[0]["cantidad"]

print("\n")
print("=" * 60)
print("VALIDACION TRACKING")
print("=" * 60)

print(
    f"Bronze Tracking: {tracking_bronze}"
)

print(
    f"Silver Tracking: {tracking_silver}"
)


# ============================================================
# RESULTADO FINAL
# ============================================================

if errores:

    print("\n")
    print("=" * 60)
    print("VALIDACION FALLIDA")
    print("=" * 60)

    for error in errores:
        print(error)

    raise Exception(
        "Existen tablas Gold sin registros."
    )


print("\n")
print("=" * 60)
print("VALIDACION FINALIZADA CORRECTAMENTE")
print("=" * 60)
print(
    "Todas las tablas Gold contienen registros."
)