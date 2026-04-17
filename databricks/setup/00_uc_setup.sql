-- ═══════════════════════════════════════════════════════════════
-- Unity Catalog Setup DDL
-- Creates catalog, schemas, and external volume for the
-- Construction & Oil/Gas Procurement Lakehouse
-- ═══════════════════════════════════════════════════════════════

-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS procurement_dev
COMMENT 'Construction & Oil/Gas Procurement Lakehouse – Development';

USE CATALOG procurement_dev;

-- 2. Create Schemas (medallion layers + semantic + data quality)
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion layer – 1:1 copy from source files';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleansed and conformed layer – dedup, type cast, DQ rules';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready layer – star schema with SCD2 dimensions and fact tables';

CREATE SCHEMA IF NOT EXISTS semantic
COMMENT 'Materialized view data cubes for analytical consumption';

CREATE SCHEMA IF NOT EXISTS dq
COMMENT 'Data quality quarantine and metrics tables';

-- 3. Create External Volume for raw data landing
-- Update the STORAGE LOCATION to match your ADLS Gen2 container path
CREATE VOLUME IF NOT EXISTS bronze.procurement_data
COMMENT 'Landing zone for generated CSV/Excel source files'
-- LOCATION 'abfss://<container>@<storage_account>.dfs.core.windows.net/procurement_data'
;

-- 4. Grant permissions (adjust principals as needed)
-- GRANT USE CATALOG   ON CATALOG procurement_dev TO `data-engineers`;
-- GRANT USE SCHEMA    ON SCHEMA bronze    TO `data-engineers`;
-- GRANT USE SCHEMA    ON SCHEMA silver    TO `data-engineers`;
-- GRANT USE SCHEMA    ON SCHEMA gold      TO `data-engineers`;
-- GRANT USE SCHEMA    ON SCHEMA semantic  TO `data-analysts`;
-- GRANT USE SCHEMA    ON SCHEMA dq        TO `data-engineers`;
-- GRANT SELECT        ON SCHEMA semantic  TO `data-analysts`;
-- GRANT SELECT        ON SCHEMA gold      TO `data-analysts`;

-- 5. Verify setup
SHOW SCHEMAS IN procurement_dev;
