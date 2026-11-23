# ElectroCasa Lakehouse

## Proyecto Integrador Databricks

Plataforma de datos para ElectroCasa, cadena ficticia de electrodomésticos con 40 sucursales en Perú.

El objetivo del proyecto es construir una solución de datos de punta a punta sobre Azure Databricks utilizando arquitectura Medallion, Unity Catalog, Lakeflow Spark Declarative Pipelines, Lakeflow Jobs y Declarative Automation Bundles.

---

## Arquitectura

La solución utiliza una arquitectura Medallion compuesta por:

- Bronze: ingesta de datos en formato original con columnas técnicas de auditoría.
- Silver: limpieza, normalización, deduplicación, reglas de calidad y cuarentena.
- Gold: tablas analíticas orientadas a las preguntas de negocio.

Flujo general:

Fuentes -> Landing Volume -> Bronze -> Silver -> Gold

Los registros que no cumplen reglas críticas son enviados a una tabla de cuarentena para mantener trazabilidad.

---

## Fuentes de datos

| Fuente | Formato | Método de ingesta |
|---|---|---|
| Ventas | CSV | Auto Loader |
| Empleados | CSV | Auto Loader |
| Reseñas | JSON Lines | Auto Loader |
| Catálogo | JSON | COPY INTO |
| Devoluciones | CSV | COPY INTO |
| Sucursales | CSV | COPY INTO |
| Tracking de envíos | Azure SQL Database | Lakeflow Connect |

---

## Unity Catalog

Catálogo utilizado:

`electrocasa_dev`

Schemas:

- `bronze`
- `silver`
- `gold`
- `default`

Volume de Landing:

`/Volumes/electrocasa_dev/default/vol_landing/`

---

## Evidencia de procesamiento - Lote 1

| Fuente | Lote | Registros |
|---|---|---:|
| Ventas | lote_01 | 6783 |
| Empleados | lote_01 | 2257 |
| Devoluciones | lote_01 | 1387 |
| Sucursales | lote_01 | 40 |
| Catálogo | lote_01 | 2305 |
| Reseñas | lote_01 | 5292 |

Los conteos corresponden a los archivos inicialmente cargados en el Landing Volume antes de iniciar el procesamiento Bronze/Silver.

---

## Calidad de datos

Las principales reglas definidas son:

- Ventas: `monto_total > 0`.
- Catálogo: `precio_lista > 0`.
- Empleados: `id_empleado` y `fecha_evento` obligatorios.
- Reseñas: `calificacion BETWEEN 1 AND 5`.
- Devoluciones: `monto_reembolso >= 0` y no mayor al monto de la venta.
- Tracking: estados válidos `pendiente`, `en_transito`, `entregado`, `devuelto`.

Los registros que incumplan reglas críticas serán almacenados en `silver.cuarentena`.

---

## Empleados - SCD Type 2

La información de empleados será historizada mediante SCD Type 2.

El orden de los eventos utilizará:

- `fecha_evento`
- `fecha_registro`

Esto permitirá manejar correctamente transferencias, cambios salariales, bajas y eventos registrados tardíamente.

---

## Seguridad

Grupos definidos:

- `electrocasa_ingenieria`
- `electrocasa_analistas`
- `electrocasa_auditoria`

El acceso a DNI y salario será restringido mediante masking.

---

## Estado del proyecto

- [x] Unity Catalog configurado
- [x] Schemas creados
- [x] Landing Volume creado
- [x] Grupos creados
- [x] Lote 1 cargado
- [x] Exploración inicial realizada
- [x] Contratos de datos creados
- [ ] Ingesta Bronze
- [ ] Transformaciones Silver
- [ ] SCD Type 2 Empleados
- [ ] Cuarentena
- [ ] Tablas Gold
- [ ] Lakeflow Connect Tracking
- [ ] Lakeflow Job
- [ ] Automation Bundle
- [ ] Lote 2
- [ ] Lote 3
- [ ] Evidencias finales