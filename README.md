# ElectroCasa Lakehouse

## Proyecto Integrador Databricks

Plataforma de datos para **ElectroCasa**, cadena ficticia de electrodomésticos con 40 sucursales en Perú.

El objetivo del proyecto es implementar una solución de datos de punta a punta sobre Azure Databricks utilizando:

- Arquitectura Medallion.
- Unity Catalog.
- Auto Loader.
- COPY INTO.
- Lakeflow Spark Declarative Pipelines.
- Lakeflow Connect.
- Lakeflow Jobs.
- Declarative Automation Bundles.
- Gobierno y seguridad con Unity Catalog.

---

# 1. Arquitectura

La solución implementa una arquitectura **Medallion** compuesta por tres capas:

- **Bronze:** ingesta de datos en formato original con información técnica de auditoría.
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

Los registros que no cumplen las reglas críticas de calidad no son eliminados silenciosamente. Se almacenan en tablas de cuarentena para mantener trazabilidad.

---

# 2. Fuentes de datos

| Fuente | Formato | Método de ingesta |
|---|---|---|
| Ventas | CSV | Auto Loader |
| Empleados | CSV | Auto Loader |
| Reseñas | JSON Lines | Auto Loader |
| Catálogo | JSON | COPY INTO |
| Devoluciones | CSV | COPY INTO |
| Sucursales | CSV | COPY INTO |
| Tracking de envíos | Azure SQL Database | Lakeflow Connect Query-Based |

La solución utiliza más de un mecanismo de ingesta con el objetivo de aplicar diferentes patrones según la naturaleza de cada fuente.

---

# 3. Unity Catalog

Catálogo del proyecto:

```text
electrocasa_dev
```

Schemas utilizados:

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

Estructura:

```text
vol_landing/
├── ventas/
├── catalogo/
├── empleados/
├── resenas/
├── devoluciones/
└── sucursales/
```

Las tablas del proyecto se administran mediante Unity Catalog.

---

# 4. Ingesta con Auto Loader

Auto Loader se utilizó para las fuentes que llegan incrementalmente por archivos:

```text
Ventas
Empleados
Reseñas
```

La implementación utiliza `cloudFiles` dentro de Lakeflow Spark Declarative Pipelines.

Para las fuentes CSV se definieron schemas explícitos en lugar de utilizar `inferSchema`.

Las tablas Bronze principales son:

```text
electrocasa_dev.bronze.bronze_ventas
electrocasa_dev.bronze.bronze_empleados
electrocasa_dev.bronze.bronze_resenas
```

---

# 5. Ingesta con COPY INTO

COPY INTO se utilizó para:

```text
Catálogo
Devoluciones
Sucursales
```

La implementación se encuentra en:

```text
notebooks/01_copy_into.py
```

El proceso es idempotente: volver a procesar los mismos archivos no genera una nueva carga de los archivos previamente registrados.

Conteos Bronze finales:

| Fuente | Registros |
|---|---:|
| Catálogo | 4,705 |
| Devoluciones | 4,217 |
| Sucursales | 40 |

---

# 6. Tracking de envíos con Lakeflow Connect

La fuente de Tracking se encuentra en Azure SQL Database.

Se utilizó Lakeflow Connect con captura **Query-Based**.

Configuración:

```text
Tabla origen:
dbo.TrackingEnvios

Tabla destino:
electrocasa_dev.bronze.tracking_envios

Cursor:
fecha_modificacion

Primary Key:
evento_id
```

Columnas utilizadas:

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

Se realizaron al menos dos ejecuciones consecutivas del conector.

Conteo observado después de la segunda ejecución:

```text
15139 registros
```

La segunda ejecución mantuvo el mismo conteo, demostrando que no se produjo una duplicación de los registros ya procesados.

---

# 7. Normalización de Tracking

La fuente contenía diferentes formas de representar el mismo estado.

Ejemplos encontrados:

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

En Silver fueron estandarizados a:

```text
pendiente
en_transito
entregado
devuelto
```

Los couriers también presentaban variantes:

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

Tabla Silver:

```text
electrocasa_dev.silver.tracking_envios
```

Conteo final:

```text
15139 registros
```

---

# 8. Calidad de datos

Se implementaron reglas de calidad para identificar información inválida.

Principales reglas:

- Ventas: `monto_total > 0`.
- Catálogo: `precio_lista > 0`.
- Empleados: `id_empleado` y `fecha_evento` obligatorios.
- Reseñas: `calificacion BETWEEN 1 AND 5`.
- Devoluciones: `monto_reembolso >= 0` y no mayor al monto de la venta asociada.
- Tracking: estados válidos `pendiente`, `en_transito`, `entregado`, `devuelto`.

Tablas de cuarentena:

```text
silver.cuarentena_ventas
silver.cuarentena_catalogo
silver.cuarentena_devoluciones
silver.cuarentena_resenas
silver.cuarentena_empleados
silver.cuarentena_tracking
```

Los registros inválidos permanecen disponibles para auditoría y trazabilidad.

---

# 9. Empleados - SCD Type 2

La información de empleados se historizó utilizando **SCD Type 2**.

Tabla resultante:

```text
electrocasa_dev.silver.empleados_scd2
```

Llave:

```text
id_empleado
```

El orden de los eventos utiliza información proveniente de la fuente:

```text
fecha_evento
fecha_registro
```

Se utiliza una secuencia compuesta para ordenar correctamente los eventos:

```python
sequence_by = F.struct(
    F.col("fecha_evento"),
    F.col("fecha_registro")
)
```

Esto permite manejar correctamente:

- Altas.
- Transferencias.
- Cambios salariales.
- Ceses.
- Múltiples eventos el mismo día.
- Eventos registrados tardíamente.

De esta manera se puede determinar en qué sucursal se encontraba un empleado en una fecha histórica.

---

# 10. Procesamiento por lotes

Los archivos fueron incorporados de forma secuencial para demostrar procesamiento incremental.

## Lote 1

| Tabla | Registros |
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

### Rechazados Lote 1

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

| Tabla | Registros |
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

### Rechazados Lote 2

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

| Tabla | Registros |
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

### Rechazados Lote 3

| Fuente | Rechazados |
|---|---:|
| Ventas | 353 |
| Catálogo | 94 |
| Devoluciones | 72 |
| Reseñas | 103 |
| Empleados | 38 |
| Tracking | 0 |

---

# 11. Análisis de incrementalidad

Los tres lotes permiten comprobar el comportamiento incremental del Lakehouse.

Entre el Lote 1 y el Lote 3:

```text
Bronze Ventas:
6,783 → 18,240

Silver Ventas:
6,656 → 17,653

Bronze Empleados:
2,257 → 4,448

Silver Empleados SCD2:
2,121 → 4,049

Bronze Devoluciones:
1,387 → 4,217

Silver Devoluciones:
1,373 → 4,119
```

Reseñas permanece sin cambios durante el Lote 3 porque la fuente dispone únicamente de dos lotes.

Catálogo también permanece sin cambios durante el Lote 3 porque dispone de dos snapshots.

Sucursales permanece en 40 registros.

Tracking permanece en 15,139 eventos debido a que la fuente Azure SQL no recibió nuevos registros durante las ejecuciones realizadas.

En Catálogo se observa además el comportamiento esperado de los snapshots:

```text
Bronze catálogo: 4705
Silver catálogo: 2349
```

Bronze conserva los snapshots recibidos mientras Silver consolida y deduplica los productos.

---

# 12. Capa Gold

La capa Gold responde las preguntas de negocio mediante las siguientes tablas:

```text
gold.ventas_netas_sucursal_mes
gold.top10_productos_unidades
gold.top10_productos_tasa_devolucion
gold.dotacion_mensual_sucursal
gold.ventas_vendedor_mes
gold.resenas_negativas_categoria_mes
gold.despachos_courier
```

Conteos finales:

| Tabla Gold | Registros |
|---|---:|
| ventas_netas_sucursal_mes | 328 |
| top10_productos_unidades | 10 |
| top10_productos_tasa_devolucion | 10 |
| dotacion_mensual_sucursal | 320 |
| ventas_vendedor_mes | 5,531 |
| resenas_negativas_categoria_mes | 72 |
| despachos_courier | 3 |

Todas las tablas Gold contienen registros.

---

# 13. Ventas por vendedor

Para esta pregunta solamente se consideran las ventas de tienda física.

Durante la validación se detectó que el valor real normalizado del canal era:

```text
TIENDA_FISICA
```

Cantidad de ventas físicas identificadas:

```text
12409
```

Inicialmente la tabla:

```text
gold.ventas_vendedor_mes
```

quedó con cero registros debido a que el filtro no incluía este valor.

La tarea de validación detectó el problema y detuvo correctamente el Job.

Luego se corrigió la transformación Gold para incorporar:

```text
TIENDA_FISICA
```

Resultado final:

```text
gold.ventas_vendedor_mes = 5531 registros
```

Este incidente permitió demostrar el funcionamiento efectivo de la validación automática del Job.

---

# 14. Despachos por Courier

Resultado Gold obtenido:

| Courier | Tiempo promedio horas | Entregados | Devueltos | Estado final | % devueltos |
|---|---:|---:|---:|---:|---:|
| SERVIENTREGA | 103.83 | 1,363 | 113 | 1,476 | 7.66 |
| OLVA COURIER | 103.86 | 1,391 | 85 | 1,476 | 5.76 |
| SHALOM | 102.43 | 1,399 | 85 | 1,484 | 5.73 |

El tiempo promedio considera el intervalo entre:

```text
primer evento pendiente
        ↓
primer evento entregado
```

utilizando `fecha_modificacion`.

Antes del cálculo se eliminan los mensajes reprocesados correspondientes al mismo cambio de estado.

---

# 15. Lakeflow Spark Declarative Pipeline

Pipeline principal:

```text
electrocasa_pipeline_dev
```

Archivos de transformación:

```text
src/electrocasa/transformations/bronze.py
src/electrocasa/transformations/silver.py
src/electrocasa/transformations/gold.py
```

El pipeline procesa:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

La ejecución final del pipeline terminó correctamente.

---

# 16. Orquestación con Lakeflow Jobs

Job implementado:

```text
electrocasa_job_dev
```

Flujo de tareas:

```text
copy_into_bronze
        ↓
tracking_ingestion
        ↓
pipeline_medallion
        ↓
validacion_final
```

Se utilizaron distintos tipos de tarea:

- Python script.
- Lakeflow Connect Pipeline.
- Lakeflow Spark Declarative Pipeline.
- Python de validación.

La tarea `validacion_final` realiza:

1. Conteo de tablas Silver.
2. Conteo de tablas Gold.
3. Conteo de registros rechazados.
4. Validación de Tracking.
5. Fallo intencional si alguna tabla Gold queda vacía.

Configuración:

```text
max_concurrent_runs = 1
reintentos = 1
alertas por correo ante fallo
```

Schedule:

```text
23:00
America/Lima
```

## Ejecución final del Job

Resultado:

```text
copy_into_bronze     ✅ Succeeded
tracking_ingestion   ✅ Succeeded
pipeline_medallion   ✅ Succeeded
validacion_final     ✅ Succeeded
```

Estado final:

```text
SUCCEEDED
```

La ejecución reparada permitió reutilizar las tareas previamente exitosas y volver a ejecutar únicamente las tareas necesarias.

---

# 17. Declarative Automation Bundle

El proyecto utiliza Databricks Declarative Automation Bundles.

Archivo raíz:

```text
databricks.yml
```

Recursos:

```text
resources/electrocasa_pipeline.yml
resources/electrocasa_job.yml
```

Targets configurados:

```text
dev
prod
```

Validación ejecutada:

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

El despliegue creó o actualizó:

```text
electrocasa_pipeline_dev
electrocasa_job_dev
```

También se ejecutó el Job mediante CLI:

```bash
databricks bundle run -t dev electrocasa_job
```

---

# 18. Monitoreo

El monitoreo se realizó mediante las interfaces de Databricks:

- Pipeline Graph.
- Tablas y expectativas del pipeline.
- Historial de ejecuciones.
- Graph del Lakeflow Job.
- Estado individual de cada task.
- Output de la tarea de validación.

La validación automática permitió detectar una tabla Gold vacía durante una ejecución y detener el Job antes de considerarlo exitoso.

Luego de corregir la transformación, la ejecución final terminó con todas las tareas en estado `Succeeded`.

---

# 19. Compute

El proyecto fue desarrollado utilizando **Databricks Free Edition**.

Se utilizó compute Serverless debido a:

- ausencia de administración manual de clusters;
- inicio automático de recursos;
- integración con Jobs y Pipelines;
- cargas de trabajo periódicas;
- reducción de recursos ociosos.

El SQL Editor utilizó un Serverless SQL Warehouse.

---

# 20. Gobierno y seguridad

Se crearon los grupos requeridos:

```text
electrocasa_ingenieria
electrocasa_analistas
electrocasa_auditoria
```

Modelo de acceso:

| Grupo | Acceso |
|---|---|
| electrocasa_ingenieria | Lectura y escritura sobre Bronze, Silver y Gold |
| electrocasa_analistas | Solo lectura sobre Gold |
| electrocasa_auditoria | Solo lectura sobre Gold y evidencia de auditoría |

Los permisos fueron implementados mediante Unity Catalog.

---

# 21. Evidencia GRANT / REVOKE

Para demostrar la revocación de permisos se otorgó temporalmente a:

```text
electrocasa_analistas
```

acceso sobre:

```text
electrocasa_dev.silver
```

Antes del REVOKE se observaron:

```text
electrocasa_analistas   SELECT       SCHEMA
electrocasa_analistas   USE SCHEMA   SCHEMA
```

Luego se ejecutó:

```sql
REVOKE SELECT
ON SCHEMA electrocasa_dev.silver
FROM `electrocasa_analistas`;

REVOKE USE SCHEMA
ON SCHEMA electrocasa_dev.silver
FROM `electrocasa_analistas`;
```

Después del REVOKE, `SHOW GRANTS` confirmó que los privilegios de Analistas sobre Silver ya no estaban presentes.

---

# 22. Masking de DNI y salario

Las columnas sensibles de empleados son:

```text
dni      STRING
salario  DECIMAL(18,2)
```

Se definieron funciones de masking:

```text
electrocasa_dev.default.mask_dni
electrocasa_dev.default.mask_salario
```

La lógica utiliza:

```sql
is_account_group_member('electrocasa_ingenieria')
```

Los miembros del grupo:

```text
electrocasa_ingenieria
```

pueden consultar los valores reales.

Para otros grupos, los valores sensibles se presentan enmascarados.

La definición del masking forma parte de la tabla de empleados administrada por el pipeline.

---

# 23. Auditoría y catálogo System

Se intentó otorgar a:

```text
electrocasa_auditoria
```

acceso al catálogo:

```text
system
```

mediante:

```sql
GRANT USE CATALOG
ON CATALOG system
TO `electrocasa_auditoria`;
```

Databricks Free Edition devolvió:

```text
PERMISSION_DENIED:
User does not have MANAGE on Catalog 'system'.
```

Debido a esta limitación del entorno, se utilizó como alternativa la evidencia disponible en Catalog Explorer y Lineage.

El error fue conservado como evidencia de la restricción de permisos del entorno Free Edition.

---

# 24. Contratos de datos

Se creó un contrato por fuente:

```text
contratos/
├── ventas.yml
├── catalogo.yml
├── empleados.yml
├── resenas.yml
├── devoluciones.yml
├── sucursales.yml
└── tracking_envios.yml
```

Los contratos documentan:

- origen;
- formato;
- claves;
- columnas principales;
- tipos esperados;
- reglas de calidad;
- estrategia de ingesta.

---

# 25. Estructura del repositorio

```text
electrocasa-lakehouse/
│
├── databricks.yml
│
├── README.md
│
├── resources/
│   ├── electrocasa_pipeline.yml
│   └── electrocasa_job.yml
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
│   ├── 02_validacion.py
│   └── 03_seguridad.sql
│
└── src/
    └── electrocasa/
        ├── transformations/
        │   ├── bronze.py
        │   ├── silver.py
        │   └── gold.py
        │
        └── utils/
```

---

# 26. Decisiones técnicas

## Auto Loader

Se utilizó para:

```text
Ventas
Empleados
Reseñas
```

porque son fuentes de archivos con llegada incremental.

## COPY INTO

Se utilizó para:

```text
Catálogo
Devoluciones
Sucursales
```

por tratarse de cargas batch y snapshots.

## Lakeflow Connect

Tracking se integró desde Azure SQL Database mediante captura Query-Based utilizando `fecha_modificacion` como cursor incremental.

## Empleados

Se utilizó SCD Type 2 para preservar la historia de cada empleado y poder conocer su sucursal vigente en una fecha pasada.

## Catálogo

Bronze conserva los snapshots recibidos y Silver consolida la información para evitar duplicados.

## Cuarentena

Los registros que incumplen reglas de calidad se almacenan en tablas independientes para mantener trazabilidad.

## Seguridad

El acceso se administra mediante grupos de Unity Catalog y masking de información sensible.

---

# 27. Estado final del proyecto

## Completado

- [x] Unity Catalog configurado
- [x] Schemas Bronze, Silver y Gold
- [x] Landing Volume
- [x] Grupos de seguridad
- [x] Contratos de datos
- [x] Auto Loader
- [x] COPY INTO
- [x] Lakeflow Connect
- [x] Tracking incremental
- [x] Dos ejecuciones de Tracking
- [x] Bronze
- [x] Silver
- [x] Normalización
- [x] Deduplicación
- [x] Cuarentena
- [x] SCD Type 2
- [x] Gold
- [x] Lote 1
- [x] Lote 2
- [x] Lote 3
- [x] Pipeline Medallion
- [x] Lakeflow Job
- [x] Validación final
- [x] Schedule
- [x] Reintentos
- [x] Alertas
- [x] Declarative Automation Bundle
- [x] Target Dev
- [x] Target Prod
- [x] Bundle Validate
- [x] Bundle Deploy
- [x] Bundle Run
- [x] GRANT
- [x] REVOKE
- [x] Masking DNI
- [x] Masking salario
- [x] Evidencia de limitación del catálogo System
- [x] Job final ejecutado exitosamente

---

# 28. Resultado final

La plataforma integra exitosamente las diferentes fuentes de ElectroCasa y permite disponer de información consolidada, limpia, historizada y gobernada.

El resultado final implementa:

```text
Ingesta
   ↓
Bronze
   ↓
Calidad y normalización
   ↓
Silver
   ↓
SCD Type 2
   ↓
Gold
   ↓
Orquestación
   ↓
Validación
   ↓
Gobierno y seguridad
```

La ejecución final del Job terminó correctamente con todas sus tareas en estado `Succeeded`.

---

# Repositorio

Repositorio público del proyecto:

```text
https://github.com/jrpv30/electrocasa-lakehouse
```