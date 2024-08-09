-- SCD Handler for Insert Operation: dc emi partners
BEGIN;

-- Inserting New Records
INSERT INTO edw_dim.dw_dim_dc_emi_partners (
    utid,
    btid,
    dc_emi_enabled,
    issuing_bank_code,
    version,
    date_from,
    date_to
)
SELECT
    inst.utid,
    inst.btid,
    inst.dc_emi_enabled,
    inst.issuing_bank_code,
    1 AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_dc_emi_partner_config AS inst
    LEFT JOIN edw_dim.dw_dim_dc_emi_partners AS dim ON dim.utid = inst.utid
    AND dim.btid = inst.btid
WHERE
    dim.utid IS NULL;

-- Inserting Modified Records
INSERT INTO edw_dim.dw_dim_dc_emi_partners (
    utid,
    btid,
    dc_emi_enabled,
    issuing_bank_code,
    version,
    date_from,
    date_to
)
SELECT
    inst.utid,
    inst.btid,
    inst.dc_emi_enabled,
    inst.issuing_bank_code,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.utid, inst.btid
        ORDER BY inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_dc_emi_partner_config AS inst
    LEFT JOIN edw_dim.dw_dim_dc_emi_partners AS dim ON dim.utid = inst.utid
    AND dim.btid = inst.btid
    AND dim.lastversion = true
WHERE
    dim.dc_emi_enabled <> inst.dc_emi_enabled
    OR dim.issuing_bank_code <> inst.issuing_bank_code;

-- Updating Previous Records as False and Determining Maximum Version Number for Each utid
WITH MaxVersions AS (
    SELECT
        utid,
        btid,
        MAX(dim.version) AS max_version
    FROM
        edw_dim.tmp_src_dc_emi_partner_config AS inst
        JOIN edw_dim.dw_dim_dc_emi_partners AS dim ON dim.utid = inst.utid
        AND dim.btid = inst.btid
        AND dim.date_from = inst.crtupd_dt
    GROUP BY
        dim.utid, dim.btid
)
UPDATE
    edw_dim.dw_dim_dc_emi_partners AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.utid = mv.utid
    AND dim.btid = mv.btid
    AND dim.version < mv.max_version;

-- Reordering the Dates
WITH next_entries AS (
    SELECT
        utid,
        btid,
        version,
        LEAD(date_from) OVER (
            PARTITION BY utid, btid
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.tmp_src_dc_emi_partner_config AS inst
        JOIN edw_dim.dw_dim_dc_emi_partners AS dim ON dim.utid = inst.utid
        AND dim.btid = inst.btid
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
    edw_dim.dw_dim_dc_emi_partners AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.utid = next_entries.utid
    AND dim.btid = next_entries.btid
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;
