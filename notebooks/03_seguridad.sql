-- ============================================================
-- ELECTROCASA
-- GOBIERNO Y SEGURIDAD - UNITY CATALOG
-- ============================================================
--
-- Catalogo:
--   electrocasa_dev
--
-- Grupos:
--   electrocasa_ingenieria
--   electrocasa_analistas
--   electrocasa_auditoria
--
-- IMPORTANTE:
-- El masking de empleados_scd2 se declara dentro de silver.py.
-- Este archivo crea las funciones de masking y configura
-- los permisos de Unity Catalog.
-- ============================================================


-- ============================================================
-- 1. FUNCION DE MASKING - DNI
-- ============================================================

CREATE OR REPLACE FUNCTION
electrocasa_dev.default.mask_dni(valor STRING)
RETURN
    CASE
        WHEN is_account_group_member('electrocasa_ingenieria')
            THEN valor
        ELSE '********'
    END;


-- ============================================================
-- 2. FUNCION DE MASKING - SALARIO
-- ============================================================

CREATE OR REPLACE FUNCTION
electrocasa_dev.default.mask_salario(valor DECIMAL(18,2))
RETURN
    CASE
        WHEN is_account_group_member('electrocasa_ingenieria')
            THEN valor
        ELSE CAST(NULL AS DECIMAL(18,2))
    END;


-- ============================================================
-- 3. PERMISOS - INGENIERIA
-- Lectura y escritura sobre Bronze, Silver y Gold
-- ============================================================

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_ingenieria`;


-- Acceso al schema donde se encuentran las funciones de masking

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.default
TO `electrocasa_ingenieria`;


GRANT EXECUTE
ON FUNCTION electrocasa_dev.default.mask_dni
TO `electrocasa_ingenieria`;


GRANT EXECUTE
ON FUNCTION electrocasa_dev.default.mask_salario
TO `electrocasa_ingenieria`;


-- Bronze

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.bronze
TO `electrocasa_ingenieria`;


GRANT SELECT, MODIFY
ON SCHEMA electrocasa_dev.bronze
TO `electrocasa_ingenieria`;


-- Silver

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.silver
TO `electrocasa_ingenieria`;


GRANT SELECT, MODIFY
ON SCHEMA electrocasa_dev.silver
TO `electrocasa_ingenieria`;


-- Gold

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_ingenieria`;


GRANT SELECT, MODIFY
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_ingenieria`;


-- ============================================================
-- 4. PERMISOS - ANALISTAS
-- Solo lectura sobre Gold
-- ============================================================

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_analistas`;


GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_analistas`;


GRANT SELECT
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_analistas`;


-- ============================================================
-- 5. PERMISOS - AUDITORIA
-- Solo lectura sobre Gold
-- ============================================================

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_auditoria`;


GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_auditoria`;


GRANT SELECT
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_auditoria`;


-- ============================================================
-- 6. DEMOSTRACION GRANT / REVOKE
-- ============================================================
--
-- Se otorga temporalmente a electrocasa_analistas un permiso
-- que NO le corresponde sobre Silver.
--
-- Luego se revoca y se comprueba con SHOW GRANTS.
-- ============================================================


-- ------------------------------------------------------------
-- 6.1 GRANT TEMPORAL
-- ------------------------------------------------------------

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.silver
TO `electrocasa_analistas`;


GRANT SELECT
ON SCHEMA electrocasa_dev.silver
TO `electrocasa_analistas`;


-- Evidencia ANTES del REVOKE

SHOW GRANTS
ON SCHEMA electrocasa_dev.silver;


-- ------------------------------------------------------------
-- 6.2 REVOKE
-- ------------------------------------------------------------

REVOKE SELECT
ON SCHEMA electrocasa_dev.silver
FROM `electrocasa_analistas`;


REVOKE USE SCHEMA
ON SCHEMA electrocasa_dev.silver
FROM `electrocasa_analistas`;


-- Evidencia DESPUES del REVOKE

SHOW GRANTS
ON SCHEMA electrocasa_dev.silver;


-- ============================================================
-- 7. VERIFICACION DE PERMISOS FINALES
-- ============================================================

SHOW GRANTS
ON SCHEMA electrocasa_dev.bronze;


SHOW GRANTS
ON SCHEMA electrocasa_dev.silver;


SHOW GRANTS
ON SCHEMA electrocasa_dev.gold;


-- ============================================================
-- 8. VERIFICACION DE FUNCIONES DE MASKING
-- ============================================================

SHOW FUNCTIONS
IN electrocasa_dev.default;


-- ============================================================
-- 9. VERIFICACION DEL USUARIO / GRUPO ACTUAL
-- ============================================================

SELECT
    current_user() AS usuario_actual,
    is_account_group_member(
        'electrocasa_ingenieria'
    ) AS pertenece_ingenieria;


-- ============================================================
-- 10. VERIFICACION DE EMPLEADOS
-- ============================================================
--
-- Si el usuario pertenece a electrocasa_ingenieria:
-- DNI y salario se muestran con sus valores reales.
--
-- Para otros usuarios, las columnas se muestran enmascaradas.
-- La definicion MASK se encuentra en silver.py.
-- ============================================================

SELECT
    id_empleado,
    nombre,
    dni,
    salario,
    sucursal_id,
    cargo
FROM electrocasa_dev.silver.empleados_scd2
LIMIT 20;


-- ============================================================
-- 11. AUDITORIA DEL CATALOGO SYSTEM - FREE EDITION
-- ============================================================
--
-- IMPORTANTE:
-- NO ejecutar esta seccion mediante "Run all".
--
-- En Databricks Free Edition se espera que falle porque
-- el usuario no posee MANAGE sobre el catalogo system.
--
-- Esta limitacion se documenta en el README y se utiliza
-- Catalog Explorer / Lineage como evidencia alternativa.
--
-- Para obtener la captura de evidencia ejecutar manualmente:
--
-- GRANT USE CATALOG
-- ON CATALOG system
-- TO `electrocasa_auditoria`;
--
-- Error obtenido durante el proyecto:
--
-- PERMISSION_DENIED:
-- User does not have MANAGE on Catalog 'system'.
--
-- ============================================================


-- ============================================================
-- FIN
-- ============================================================