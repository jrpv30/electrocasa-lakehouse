# ============================================================
# ELECTROCASA
# CARGAS BRONZE MEDIANTE COPY INTO
#
# Fuentes:
#   1. Catalogo       -> JSON multiline
#   2. Devoluciones   -> CSV
#   3. Sucursales     -> CSV
# ============================================================

CATALOG = "electrocasa_dev"

print("==============================================")
print("ELECTROCASA - COPY INTO BRONZE")
print("Catalogo:", CATALOG)
print("==============================================")


# ============================================================
# 1. BRONZE - CATALOGO
# ============================================================

print("\n[1/3] Preparando bronze.catalogo ...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.catalogo (
    producto_id STRING,
    nombre_producto STRING,
    categoria STRING,
    marca STRING,
    precio_lista STRING,
    activo STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

spark.sql(f"""
COPY INTO {CATALOG}.bronze.catalogo
FROM (
    SELECT
        producto_id,
        nombre_producto,
        categoria,
        marca,
        CAST(precio_lista AS STRING) AS precio_lista,
        CAST(activo AS STRING) AS activo,
        current_timestamp() AS _ingestion_timestamp,
        _metadata.file_name AS _source_file
    FROM '/Volumes/{CATALOG}/default/vol_landing/catalogo/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS (
    'multiLine' = 'true'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
)
""")

print("Catalogo procesado correctamente.")


# ============================================================
# 2. BRONZE - DEVOLUCIONES
# ============================================================

print("\n[2/3] Preparando bronze.devoluciones ...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.devoluciones (
    devolucion_id STRING,
    venta_id STRING,
    producto_id STRING,
    sucursal_id STRING,
    fecha_devolucion STRING,
    motivo STRING,
    monto_reembolso STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

spark.sql(f"""
COPY INTO {CATALOG}.bronze.devoluciones
FROM (
    SELECT
        devolucion_id,
        venta_id,
        producto_id,
        sucursal_id,
        fecha_devolucion,
        motivo,
        monto_reembolso,
        current_timestamp() AS _ingestion_timestamp,
        _metadata.file_name AS _source_file
    FROM '/Volumes/{CATALOG}/default/vol_landing/devoluciones/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = ','
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
)
""")

print("Devoluciones procesadas correctamente.")


# ============================================================
# 3. BRONZE - SUCURSALES
# ============================================================

print("\n[3/3] Preparando bronze.sucursales ...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.sucursales (
    sucursal_id STRING,
    nombre_sucursal STRING,
    distrito STRING,
    ciudad STRING,
    region STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

spark.sql(f"""
COPY INTO {CATALOG}.bronze.sucursales
FROM (
    SELECT
        sucursal_id,
        nombre_sucursal,
        distrito,
        ciudad,
        region,
        current_timestamp() AS _ingestion_timestamp,
        _metadata.file_name AS _source_file
    FROM '/Volumes/{CATALOG}/default/vol_landing/sucursales/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = ','
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
)
""")

print("Sucursales procesadas correctamente.")


# ============================================================
# VALIDACION DE CONTEOS
# ============================================================

print("\n==============================================")
print("CONTEOS BRONZE - COPY INTO")
print("==============================================")

tablas = [
    "catalogo",
    "devoluciones",
    "sucursales"
]

for tabla in tablas:

    resultado = spark.sql(
        f"""
        SELECT COUNT(*) AS cantidad
        FROM {CATALOG}.bronze.{tabla}
        """
    ).collect()

    cantidad = resultado[0]["cantidad"]

    print(f"{tabla}: {cantidad} registros")


print("\n==============================================")
print("COPY INTO FINALIZADO CORRECTAMENTE")
print("==============================================")