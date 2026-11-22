-- 0. Parameter: change the catalog here only
DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'retail_ci';

-- 1. Create Catalog
DROP CATALOG IF EXISTS IDENTIFIER(catalog_name) CASCADE;
CREATE CATALOG IF NOT EXISTS IDENTIFIER(catalog_name);
USE CATALOG IDENTIFIER(catalog_name);

-- 2. Generate Schemas (resolved against the current catalog)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 3. Create Volume / Staging Area
CREATE VOLUME IF NOT EXISTS bronze.landing;

