# ElectroCasa Lakehouse

## Proyecto Integrador Databricks

Plataforma de datos para **ElectroCasa**, cadena ficticia de electrodomésticos con 40 sucursales en Perú.

El objetivo del proyecto es construir una solución de datos de punta a punta sobre Azure Databricks utilizando arquitectura **Medallion**, Unity Catalog, Lakeflow Spark Declarative Pipelines, Lakeflow Connect, Lakeflow Jobs y Declarative Automation Bundles.

---

## 1. Arquitectura

La solución implementa una arquitectura Medallion compuesta por tres capas:

- **Bronze:** ingesta de datos en formato original con columnas técnicas de auditoría.
- **Silver:** limpieza, normalización, deduplicación, reglas de calidad, cuarentena e historización.
- **Gold:** tablas analíticas orientadas a responder las preguntas de negocio.

Flujo general:

```text
Fuentes
   ↓
Landing Volume
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

Los registros que no cumplen reglas críticas se almacenan en tablas de cuarentena para mantener trazabilidad.

---

## 2. Fuentes de datos

| Fuente | Formato | Método de ingesta |
|---|---|---|
| Ventas | CSV | Auto Loader |
| Empleados | CSV | Auto Loader |
| Reseñas | JSON Lines | Auto Loader |
| Catálogo | JSON | COPY INTO |
| Devoluciones | CSV | COPY INTO |
| Sucursales | CSV | COPY INTO |
| Tracking de envíos | Azure SQL Database | Lakeflow Connect query-based |

---

## 3. Unity Catalog

Catálogo utilizado:

```text
electrocasa_dev
```

Schemas:

```text
electrocasa_dev.bronze
electrocasa_dev.silver
electrocasa_dev.gold
electrocasa_dev.default
```

Volume de Landing:

```text
/Volumes/electrocasa_dev/default/vol_landing/
```

Subcarpetas:

```text
ventas/
catalogo/
empleados/
resenas/
devoluciones/
sucursales/
```

---

## 4. Métodos de ingesta

### Auto Loader

Se utilizó Auto Loader para las fuentes incrementales:

- Ventas
- Empleados
- Reseñas

La lectura se implementó mediante `cloudFiles` dentro de Lakeflow Spark Declarative Pipelines.

Para las fuentes CSV se utilizaron schemas explícitos.

### COPY INTO

Se utilizó `COPY INTO` para:

- Catálogo
- Devoluciones
- Sucursales

La implementación se encuentra en:

```text
notebooks/01_copy_into.py
```

`COPY INTO` permite que una nueva ejecución no vuelva a insertar archivos que ya fueron procesados.

Conteos Bronze finales:

```text
catalogo:      4705
devoluciones:  4217
sucursales:      40
```

---

## 5. Tracking de envíos

Tracking se ingiere desde Azure SQL Database utilizando Lakeflow Connect mediante captura **query-based**.

Configuración utilizada:

```text
Tabla origen:
dbo.TrackingEnvios

Destino:
electrocasa_dev.bronze.tracking_envios

Cursor:
fecha_modificacion

Primary Key:
evento_id
```

Columnas de la fuente:

```text
evento_id
tracking_id
venta_id
courier
estado_entrega
sucursal_origen
fecha_actualizacion
fecha_modificacion
```

Se ejecutó el conector dos veces consecutivas sin modificar la fuente.

Conteo observado:

```text
15139 eventos
```

La segunda ejecución mantuvo el mismo conteo, demostrando que no se produjo una recarga completa ni duplicación de los registros existentes.

---

## 6. Normalización de Tracking

Los estados encontrados en Bronze presentaban distintas variantes:

```text
PENDIENTE
pendiente
Pendiente

ENTREGADO
entregado
Entregado

En transito
EN_CAMINO
en_transito
En camino

devuelto
Devuelto
DEVUELTO
```

En Silver se normalizaron a:

```text
pendiente
en_transito
entregado
devuelto
```

Los couriers también tenían variantes:

```text
SERVIENTREGA
servientrega
Servientrega

SHALOM
shalom
Shalom

Olva
OLVA COURIER
Olva Courier
olva courier
```

Valores normalizados:

```text
SERVIENTREGA
SHALOM
OLVA COURIER
```

---

## 7. Calidad de datos

Principales reglas implementadas:

- Ventas: `monto_total > 0`.
- Catálogo: `precio_lista > 0`.
- Empleados: `id_empleado` y `fecha_evento` obligatorios.
- Reseñas: `calificacion BETWEEN 1 AND 5`.
- Devoluciones: `monto_reembolso >= 0` y no mayor al monto de la venta de origen.
- Tracking: estados dentro del conjunto `pendiente`, `en_transito`, `entregado`, `devuelto`.

Tablas de cuarentena:

```text
silver.cuarentena_ventas
silver.cuarentena_catalogo
silver.cuarentena_devoluciones
silver.cuarentena_resenas
silver.cuarentena_empleados
silver.cuarentena_tracking
```

Los registros inválidos no son eliminados silenciosamente; permanecen disponibles para análisis y trazabilidad.

---

## 8. Empleados - SCD Type 2

La información de empleados fue historizada mediante **SCD Type 2**.

Llave:

```text
id_empleado
```

El orden de eventos utiliza información proveniente de la fuente:

```text
fecha_evento
fecha_registro
```

Esto permite manejar:

- altas;
- transferencias;
- cambios salariales;
- ceses;
- múltiples eventos el mismo día;
- eventos registrados tardíamente.

Tabla resultante:

```text
electrocasa_dev.silver.empleados_scd2
```

---

# 9. Procesamiento por lotes

Los archivos fueron procesados secuencialmente para demostrar comportamiento incremental y deduplicación.

## Lote 1

| Tabla | Cantidad |
|---|---:|
| bronze_ventas | 6,783 |
| silver_ventas | 6,656 |
| bronze_empleados | 2,257 |
| silver_empleados_scd2 | 2,121 |
| bronze_resenas | 5,292 |
| silver_resenas | 5,226 |
| bronze_catalogo | 2,305 |
| silver_catalogo | 2,238 |
| bronze_devoluciones | 1,387 |
| silver_devoluciones | 1,373 |
| bronze_sucursales | 40 |
| silver_sucursales | 40 |
| bronze_tracking | 15,139 |
| silver_tracking | 15,139 |

### Rechazados - Lote 1

| Fuente | Rechazados |
|---|---:|
| Ventas | 127 |
| Catálogo | 43 |
| Devoluciones | 23 |
| Reseñas | 66 |
| Empleados | 18 |
| Tracking | 0 |

---

## Lote 2

| Tabla | Cantidad |
|---|---:|
| bronze_ventas | 13,622 |
| silver_ventas | 13,241 |
| bronze_empleados | 3,524 |
| silver_empleados_scd2 | 3,242 |
| bronze_resenas | 9,280 |
| silver_resenas | 9,097 |
| bronze_catalogo | 4,705 |
| silver_catalogo | 2,349 |
| bronze_devoluciones | 3,091 |
| silver_devoluciones | 3,034 |
| bronze_sucursales | 40 |
| silver_sucursales | 40 |
| bronze_tracking | 15,139 |
| silver_tracking | 15,139 |

### Rechazados - Lote 2

| Fuente | Rechazados |
|---|---:|
| Ventas | 263 |
| Catálogo | 94 |
| Devoluciones | 51 |
| Reseñas | 103 |
| Empleados | 29 |
| Tracking | 0 |

---

## Lote 3

| Tabla | Cantidad |
|---|---:|
| bronze_ventas | 18,240 |
| silver_ventas | 17,653 |
| bronze_empleados | 4,448 |
| silver_empleados_scd2 | 4,049 |
| bronze_resenas | 9,280 |
| silver_resenas | 9,097 |
| bronze_catalogo | 4,705 |
| silver_catalogo | 2,349 |
| bronze_devoluciones | 4,217 |
| silver_devoluciones | 4,119 |
| bronze_sucursales | 40 |
| silver_sucursales | 40 |
| bronze_tracking | 15,139 |
| silver_tracking | 15,139 |

### Rechazados - Lote 3

| Fuente | Rechazados |
|---|---:|
| Ventas | 353 |
| Catálogo | 94 |
| Devoluciones | 72 |
| Reseñas | 103 |
| Empleados | 38 |
| Tracking | 0 |

### Observaciones

Los resultados permiten comprobar el procesamiento incremental:

- Ventas, empleados y devoluciones aumentan en el Lote 3.
- Reseñas no cambia en el Lote 3 porque la fuente dispone de dos lotes.
- Catálogo no cambia en el Lote 3 porque dispone de dos snapshots.
- Sucursales permanece en 40 registros.
- Tracking permanece en 15,139 registros porque la fuente Azure SQL no recibió nuevos eventos durante las ejecuciones.

También se observa que Bronze puede contener varias versiones de un mismo producto debido a los snapshots, mientras Silver mantiene el conjunto consolidado y deduplicado.

---

# 10. Capa Gold

Se implementaron las siguientes tablas:

```text
gold.ventas_netas_sucursal_mes
gold.top10_productos_unidades
gold.top10_productos_tasa_devolucion
gold.dotacion_mensual_sucursal
gold.ventas_vendedor_mes
gold.resenas_negativas_categoria_mes
gold.despachos_courier
```

Las tablas responden las preguntas de negocio relacionadas con:

1. Ventas netas por sucursal y mes.
2. Productos más vendidos.
3. Productos con mayor tasa de devolución.
4. Dotación histórica por sucursal.
5. Ventas por vendedor.
6. Reseñas negativas.
7. Indicadores de despachos por courier.

---

## 11. Despachos por Courier

Resultados obtenidos:

| Courier | Tiempo promedio horas | Entregados | Devueltos | Estado final | % devueltos |
|---|---:|---:|---:|---:|---:|
| SERVIENTREGA | 103.83 | 1,363 | 113 | 1,476 | 7.66 |
| OLVA COURIER | 103.86 | 1,391 | 85 | 1,476 | 5.76 |
| SHALOM | 102.43 | 1,399 | 85 | 1,484 | 5.73 |

El tiempo promedio considera:

```text
primer evento pendiente
        ↓
primer evento entregado
```

utilizando `fecha_modificacion`.

Antes del cálculo se eliminan mensajes reprocesados correspondientes al mismo cambio de estado registrado más de una vez.

---

# 12. Lakeflow Spark Declarative Pipelines

Pipeline principal:

```text
electrocasa_pipeline_dev
```

Archivos de transformación:

```text
src/electrocasa/transformations/bronze.py
src/electrocasa/transformations/silver.py
src/electrocasa/transformations/gold.py
src/electrocasa/transformations/tracking.py
```

El pipeline implementa:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

---

# 13. Orquestación con Lakeflow Jobs

Job:

```text
electrocasa_job_dev
```

Flujo:

```text
copy_into_bronze
        ↓
tracking_ingestion
        ↓
pipeline_medallion
        ↓
validacion_final
```

Tipos de tarea utilizados:

- Python script.
- Lakeflow Connect pipeline.
- Lakeflow Spark Declarative Pipeline.
- Python de validación.

La tarea `validacion_final` comprueba:

- conteos Silver;
- conteos Gold;
- registros rechazados;
- que ninguna tabla Gold quede vacía.

Configuración:

```text
max_concurrent_runs = 1
reintentos = 1
alerta por correo ante fallo
```

Trigger:

```text
23:00
America/Lima
```

### Evidencia de control de calidad

Durante una ejecución integral, la tarea de validación detectó:

```text
gold.ventas_vendedor_mes = 0
```

La ejecución fue detenida correctamente.

La causa encontrada fue que el valor normalizado real del canal era:

```text
TIENDA_FISICA
```

Cantidad observada:

```text
12409 registros
```

La transformación Gold fue corregida para utilizar este valor.

Esta prueba demuestra que la validación final es capaz de detener el Job cuando una tabla analítica queda vacía.

---

# 14. Declarative Automation Bundle

Archivos principales:

```text
databricks.yml

resources/
├── electrocasa_pipeline.yml
└── electrocasa_job.yml
```

Targets configurados:

```text
dev
prod
```

Validación del Bundle:

```bash
databricks bundle validate -t dev
```

Resultado:

```text
Validation OK!
```

Deployment:

```bash
databricks bundle deploy -t dev
```

Recursos administrados mediante el Bundle:

```text
electrocasa_pipeline_dev
electrocasa_job_dev
```

---

# 15. Compute

Se utilizó compute Serverless debido a que el proyecto se desarrolló sobre Databricks Free Edition.

Ventajas:

- no requiere administración manual de clusters;
- compute bajo demanda;
- menor tiempo de recursos ociosos;
- integración directa con Jobs y Pipelines.

---

# 16. Gobierno y seguridad

Grupos creados:

```text
electrocasa_ingenieria
electrocasa_analistas
electrocasa_auditoria
```

Modelo de acceso:

| Grupo | Acceso esperado |
|---|---|
| electrocasa_ingenieria | Bronze, Silver y Gold |
| electrocasa_analistas | Solo lectura sobre Gold |
| electrocasa_auditoria | Gold y evidencia de auditoría |

También se implementará:

- `GRANT`
- demostración de `REVOKE`
- masking de `dni`
- masking de `salario`

Solo los integrantes del grupo:

```text
electrocasa_ingenieria
```

podrán observar los valores reales de DNI y salario.

---

# 17. Estructura del repositorio

```text
electrocasa-lakehouse/
│
├── databricks.yml
│
├── resources/
│   ├── electrocasa_pipeline.yml
│   └── electrocasa_job.yml
│
├── src/
│   └── electrocasa/
│       ├── transformations/
│       │   ├── bronze.py
│       │   ├── silver.py
│       │   ├── gold.py
│       │   └── tracking.py
│       └── utils/
│
├── contratos/
│   ├── ventas.yml
│   ├── catalogo.yml
│   ├── empleados.yml
│   ├── resenas.yml
│   ├── devoluciones.yml
│   ├── sucursales.yml
│   └── tracking_envios.yml
│
├── notebooks/
│   ├── 00_setup
│   ├── 01_copy_into.py
│   └── 02_validacion.py
│
└── README.md
```

---

# 18. Estado del proyecto

## Completado

- [x] Unity Catalog configurado
- [x] Schemas Bronze, Silver y Gold
- [x] Landing Volume
- [x] Grupos creados
- [x] Contratos de datos
- [x] Auto Loader
- [x] COPY INTO
- [x] Bronze
- [x] Silver
- [x] Cuarentena
- [x] SCD Type 2 de empleados
- [x] Lakeflow Connect Tracking
- [x] Dos corridas de Tracking
- [x] Normalización de estados y couriers
- [x] Gold
- [x] Lote 1
- [x] Lote 2
- [x] Lote 3
- [x] Declarative Automation Bundle
- [x] Pipeline desplegado
- [x] Job desplegado
- [x] Validación del Bundle

## Pendiente de cierre

- [ ] Ejecución final exitosa del Job después de la corrección de `ventas_vendedor_mes`
- [ ] GRANT / REVOKE
- [ ] Masking de DNI y salario
- [ ] Commit y Push final

---

# Repositorio

```text
https://github.com/jrpv30/electrocasa-lakehouse
```