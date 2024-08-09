BEGIN;
INSERT INTO edw_dim.dw_dim_merchant_classification (
  merchant_code, merchant_name, industry,
  chain_code, chain_name, chain_category,
  is_migrated, version, lastversion,
  date_from, date_to
)
SELECT
  inst.src_merchant_code,
  inst.src_merchant_name,
  inst.src_industry,
  inst.src_chain_code,
  inst.src_chain_name,
  inst.src_chain_category,
  inst.src_is_migrated,
  COALESCE(dim.max_version, 0) + ROW_NUMBER() OVER (
    PARTITION BY inst.src_merchant_code,
    inst.src_chain_code
    ORDER BY
      inst.src_created_on
  ) AS version,
  false AS lastversion,
  inst.src_created_on AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_merchant_classification AS inst
  LEFT JOIN (
    SELECT
      merchant_code,
      chain_code,
      MAX(version) AS max_version
    FROM
      edw_dim.dw_dim_merchant_classification
    GROUP BY
      merchant_code,
      chain_code
  ) AS dim ON dim.merchant_code = inst.src_merchant_code
  AND dim.chain_code = inst.src_chain_code;


-- Updating The Previous Record As False
UPDATE
  edw_dim.dw_dim_merchant_classification AS dim
SET
  lastversion = false
FROM
  edw_dim.tmp_src_merchant_classification AS upt
WHERE
  dim.merchant_code = upt.src_merchant_code
  AND dim.chain_code = upt.src_chain_code
  AND dim.lastversion = true;


-- Updating The Current Active Record as True
UPDATE
  edw_dim.dw_dim_merchant_classification AS dim
SET
  lastversion = true
FROM
  (
    SELECT
      merchant_code,
      chain_code,
      MAX(version) AS max_version
    FROM
      edw_dim.dw_dim_merchant_classification
    WHERE
      (merchant_code, chain_code) IN (
        SELECT
          src_merchant_code,
          src_chain_code
        FROM
          edw_dim.tmp_src_merchant_classification
      )
    GROUP BY
      merchant_code,
      chain_code
  ) AS latest
WHERE
  dim.merchant_code = latest.merchant_code
  AND dim.chain_code = latest.chain_code
  AND dim.version = latest.max_version;
 
-- Updating The Date In Correct Format
WITH next_entries AS (
  SELECT
    merchant_code,
    chain_code,
    version,
    LEAD(date_from) OVER (
      PARTITION BY merchant_code,
      chain_code
      ORDER BY
        version
    ) AS next_date_from
  FROM
    edw_dim.dw_dim_merchant_classification
  WHERE
    (merchant_code, chain_code) IN (
      SELECT
        src_merchant_code,
        src_chain_code
      FROM
        edw_dim.tmp_src_merchant_classification
    )
)
UPDATE
  edw_dim.dw_dim_merchant_classification AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.merchant_code = next_entries.merchant_code
  AND dim.chain_code = next_entries.chain_code
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;
COMMIT;


-- INSERT INTO edw_dim.dw_dim_merchant_classification (
--   merchant_code, 
--   merchant_name, 
--   industry,
--   chain_code, 
--   chain_name, 
--   chain_category,
--   is_migrated,
--   date_from, 
--   date_to
-- )
-- SELECT
--   inst.src_merchant_code,
--   inst.src_merchant_name,
--   inst.src_industry,
--   inst.src_chain_code,
--   inst.src_chain_name,
--   inst.src_chain_category,
--   inst.src_is_migrated,
--   inst.src_created_on AS date_from,
--   '2200-01-01 00:00:00.000' :: timestamp AS date_to
-- FROM
--   edw_dim.tmp_src_merchant_classification AS inst
--   LEFT JOIN edw_dim.dw_dim_merchant_classification AS dim 
--     ON dim.merchant_code = inst.src_merchant_code
--     AND dim.chain_code = inst.src_chain_code
-- WHERE
--   dim.merchant_code IS NULL;

-- -- Insert updated records with a new version number
-- INSERT INTO edw_dim.dw_dim_merchant_classification (
--   merchant_code, 
--   merchant_name, 
--   industry,
--   chain_code, 
--   chain_name, 
--   chain_category,
--   is_migrated, 
--   version, 
--   date_from, 
--   date_to
-- )
-- SELECT
--   inst.src_merchant_code,
--   inst.src_merchant_name,
--   inst.src_industry,
--   inst.src_chain_code,
--   inst.src_chain_name,
--   inst.src_chain_category,
--   inst.src_is_migrated,
--   dim.version + 1 AS version,
--     dim.max_version+ ROW_NUMBER() OVER (
--     PARTITION BY inst.src_merchant_code, inst.src_chain_code
--     ORDER BY inst.src_created_on
--   ) AS version,
--   inst.src_created_on AS date_from,
--   '2200-01-01 00:00:00.000' :: timestamp AS date_to
-- FROM
--   edw_dim.tmp_src_merchant_classification AS inst
--   LEFT JOIN edw_dim.dw_dim_merchant_classification AS dim 
--     ON dim.merchant_code = inst.src_merchant_code
--     AND dim.chain_code = inst.src_chain_code
--     AND dim.lastversion = true
-- WHERE
--   dim.merchant_name <> inst.src_merchant_name
--   OR dim.industry <> inst.src_industry
--   OR dim.chain_name <> inst.src_chain_name
--   OR dim.chain_category <> inst.src_chain_category
--   OR dim.is_migrated <> inst.src_is_migrated;

BEGIN;

-- Insert new records where (merchant_code) does not already exist in the target table
INSERT INTO edw_dim.dw_dim_merchant (
  merchant_code, 
  merchant_name, 
  industry,
  chain_code, 
  chain_name, 
  chain_category,
  is_migrated,
    "version",
    lastversion,--value add for version ands last version
  date_from, 
  date_to
)
SELECT
  inst.merchant_code,
  inst.merchant_name,
  inst.industry,
  inst.chain_code,
  inst.chain_name,
  inst.chain_category,
  inst.is_migrated,
  1 as  "version",
  true AS lastversion,
  inst.created_on AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_merchant AS inst
  LEFT JOIN edw_dim.dw_dim_merchant AS dim 
    ON dim.merchant_code = inst.merchant_code
    AND dim.chain_code = inst.chain_code  --chain code
WHERE
  dim.merchant_code IS NULL;

-- Insert updated records with a new version number
INSERT INTO edw_dim.dw_dim_merchant (
  merchant_code, 
  merchant_name, 
  industry,
  chain_code, 
  chain_name, 
  chain_category,
  is_migrated, 
  version, 
  date_from, 
  date_to
)
SELECT
  inst.merchant_code,
  inst.merchant_name,
  inst.industry,
  inst.chain_code,
  inst.chain_name,
  inst.chain_category,
  inst.is_migrated,
  dim.version + ROW_NUMBER() OVER (
    PARTITION BY inst.merchant_code
    ORDER BY inst.created_on --src_ should be added
  ) AS version,
  inst.created_on AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_merchant AS inst
  LEFT JOIN edw_dim.dw_dim_merchant AS dim 
    ON dim.merchant_code = inst.merchant_code
    AND dim.chain_code = inst.chain_code
    AND dim.lastversion = true
WHERE
  dim.merchant_name <> inst.merchant_name
  OR dim.industry <> inst.industry
  OR dim.chain_name <> inst.chain_name
  OR dim.chain_category <> inst.chain_category
  OR dim.is_migrated <> inst.is_migrated;

-- Update previous records to lastversion = false and determine the maximum version number for each merchant_code
WITH MaxVersions AS (
  SELECT
    merchant_code,
    chain_code,
    MAX(dim.version) AS max_version
  FROM
    edw_dim.tmp_src_merchant AS inst
    JOIN edw_dim.dw_dim_merchant AS dim 
      ON dim.merchant_code = inst.merchant_code
      AND dim.chain_code = inst.chain_code 
      AND dim.date_from = inst.created_on
  GROUP BY
    dim.merchant_code,
    dim.chain_code
)
UPDATE
  edw_dim.dw_dim_merchant dim
SET
  lastversion = false
FROM
  MaxVersions mv
WHERE
  dim.merchant_code = mv.merchant_code
  AND dim.chain_code = mv.chain_code
  AND dim.version < mv.max_version;

-- Reorder the dates: Update the date_to field in previous records to reflect the start date of the next record
WITH next_entries AS (
  SELECT
    merchant_code,
    chain_code,
    version,
    LEAD(date_from) OVER (
      PARTITION BY merchant_code,chain_code
      ORDER BY version
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_merchant as inst
  JOIN edw_dim.dw_dim_merchant as dim ON dim.merchant_code = inst.merchant_code
      AND dim.chain_code = inst.chain_code
      AND dim.date_from = inst.created_on
)
UPDATE
  edw_dim.dw_dim_merchant AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.merchant_code = next_entries.merchant_code
  AND dim.chain_code = next_entries.chain_code
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;

COMMIT;
