BEGIN;

-- Insert new records which do not exist in the dimension table
INSERT INTO edw_dim.dw_dim_utid (
    utid,
    primary_communication_id,
    secondary_communication_id,
    installationdate,
    general_purpose_utid,
    store_id,
    version,
    lastversion,
    date_from,
    date_to
)
SELECT
    inst.utid,    
    inst.primary_communication_id,      
    inst.secondary_communication_id,      
    inst.installationdate,      
    inst.store_id,          
    1 AS version,
    true AS lastversion,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_dim_unipay_terminal_lookup AS inst
LEFT JOIN
    edw_dim.dw_dim_utid AS dim
ON
    dim.utid = inst.utid
WHERE
    dim.utid IS NULL;

-- Insert updated records with a new version number and set lastversion = true
INSERT INTO edw_dim.dw_dim_utid (
    utid,
    primary_communication_id,
    secondary_communication_id,
    installationdate,
    general_purpose_utid,
    store_id,
    version,
    lastversion,
    date_from,
    date_to
)
SELECT
    inst.utid,
    inst.primary_communication_id,
    inst.secondary_communication_id,
    inst.installationdate,
    inst.store_id,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.utid
        ORDER BY inst.crtupd_dt
    ) AS version,
    true AS lastversion,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_dim_unipay_terminal_lookup AS inst
LEFT JOIN
    edw_dim.dw_dim_utid AS dim
ON
    dim.utid = inst.utid
    AND dim.lastversion = true
WHERE
    (dim.primary_communication_id <> inst.primary_communication_id
    OR dim.secondary_communication_id <> inst.secondary_communication_id
    OR dim.installationdate <> inst.installationdate
    OR dim.store_id <> inst.store_id);

-- Update previous records to false and determine the maximum version number for each utid
WITH MaxVersions AS (
  SELECT
    utid,
    MAX(dim.version) AS max_version
  FROM
    edw_dim.tmp_src_dim_unipay_terminal_lookup AS inst
    JOIN edw_dim.dw_dim_utid AS dim ON dim.utid = inst.utid
    AND dim.date_from = inst.crtupd_dt
  GROUP BY
    dim.utid
)
UPDATE
  edw_dim.dw_dim_utid AS dim
SET
  lastversion = false
FROM
  MaxVersions mv
WHERE
  dim.utid = mv.utid
  AND dim.version < mv.max_version;


-- Reordering the date_to field based on the version order
WITH next_entries AS (
  SELECT
    utid,
    version,
    LEAD(date_from) OVER (
      PARTITION BY utid
      ORDER BY version
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_dim_unipay_terminal_lookup AS inst
    JOIN edw_dim.dw_dim_utid AS dim ON dim.utid = inst.utid
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
  edw_dim.dw_dim_utid AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.utid = next_entries.utid
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;

COMMIT;
