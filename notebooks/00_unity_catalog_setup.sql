-- ===========================================================================
-- 00_unity_catalog_setup.sql
-- Phase 3 — Unity Catalog: catalog, schemas, and PII tags
--
-- Run in Databricks SQL Editor AFTER workspace is Premium tier
-- and Unity Catalog metastore is attached.
--
-- Reference:
--   Databricks UI → Data → + (Create catalog) to create via UI first,
--   then run this script for schemas and tags.
-- ===========================================================================

-- CONFIGURATION: Replace 'telecon_dev' below with your Unity Catalog catalog name.
-- Create the catalog first in Databricks UI: Catalog → + Create catalog → Default Storage.
-- Then run this script for schemas, tags, and grants.

-- Step 1: Switch to your catalog
USE CATALOG telecon_dev;  -- TODO: replace with your catalog name

-- Step 2: Create schemas (medallion layers)
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested data — append-only, schema-on-read';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned, PII-masked, quality-gated data';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business aggregates — BI and ML ready';

CREATE SCHEMA IF NOT EXISTS quarantine
  COMMENT 'Records that failed quality checks in Silver transform';

-- Step 3: Verify
SHOW SCHEMAS IN telecon_dev;  -- TODO: replace with your catalog name

-- ===========================================================================
-- Run AFTER silver notebooks have created silver.cdr table:
-- Tag PII columns so Unity Catalog lineage shows sensitivity
-- ===========================================================================

-- ALTER TABLE silver.cdr
--   ALTER COLUMN caller_msisdn_hashed
--   SET TAGS ('pii' = 'pseudonymised', 'pii_original' = 'msisdn');

-- ALTER TABLE silver.cdr
--   ALTER COLUMN callee_msisdn_hashed
--   SET TAGS ('pii' = 'pseudonymised', 'pii_original' = 'msisdn');

-- ===========================================================================
-- Run AFTER all gold tables exist — grant read access to BI role
-- ===========================================================================

-- CREATE ROLE IF NOT EXISTS telecom_analyst;
-- GRANT USE CATALOG ON CATALOG telecon_dev TO telecom_analyst;  -- TODO: your catalog
-- GRANT USE SCHEMA ON SCHEMA gold TO telecom_analyst;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO telecom_analyst;
