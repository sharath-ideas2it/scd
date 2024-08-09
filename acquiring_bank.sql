-- SCD Handler for Insert Operation: acquiring_bank
BEGIN;

-- Inserting New Records
INSERT INTO edw_dim.dw_dim_acquiring_bank (
    acq_bank_code,
    acq_bank_name,
    country_id,
    primary_ipaddress,
    primary_portno,
    secondary_ipaddress,
    secondary_portno,
    primary_flag,
    secondary_flag,
    acq_bank_desc,
    acq_bank_disclaimer,
    isactive,
    crtupd_reason,
    crtupd_status,
    crtupd_user,
    date_from,
    date_to
)
SELECT
    inst.acq_bank_code,
    inst.acq_bank_name,
    inst.country_id,
    inst.primary_ipaddress,
    inst.primary_portno,
    inst.secondary_ipaddress,
    inst.secondary_portno,
    inst.primary_flag,
    inst.secondary_flag,
    inst.acq_bank_desc,
    inst.acq_bank_disclaimer,
    inst.isactive,
    inst.crtupd_reason,
    inst.crtupd_status,
    inst.crtupd_user,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_acquiring_bank AS inst
    LEFT JOIN edw_dim.dw_dim_acquiring_bank AS dim ON dim.acq_bank_code = inst.acq_bank_code
WHERE
    dim.acq_bank_code IS NULL;

-- Inserting Modified Records
INSERT INTO edw_dim.dw_dim_acquiring_bank (
    acq_bank_code,
    acq_bank_name,
    country_id,
    primary_ipaddress,
    primary_portno,
    secondary_ipaddress,
    secondary_portno,
    primary_flag,
    secondary_flag,
    acq_bank_desc,
    acq_bank_disclaimer,
    isactive,
    crtupd_reason,
    crtupd_status,
    crtupd_user,
    version,
    date_from,
    date_to
)
SELECT
    inst.acq_bank_code,
    inst.acq_bank_name,
    inst.country_id,
    inst.primary_ipaddress,
    inst.primary_portno,
    inst.secondary_ipaddress,
    inst.secondary_portno,
    inst.primary_flag,
    inst.secondary_flag,
    inst.acq_bank_desc,
    inst.acq_bank_disclaimer,
    inst.isactive,
    inst.crtupd_reason,
    inst.crtupd_status,
    inst.crtupd_user,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.acq_bank_code
        ORDER BY inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_acquiring_bank AS inst
    LEFT JOIN edw_dim.dw_dim_acquiring_bank AS dim ON dim.acq_bank_code = inst.acq_bank_code
    AND dim.lastversion = true
WHERE
    dim.acq_bank_name <> inst.acq_bank_name
    OR dim.primary_ipaddress <> inst.primary_ipaddress
    OR dim.primary_portno <> inst.primary_portno
    OR dim.secondary_ipaddress <> inst.secondary_ipaddress
    OR dim.secondary_portno <> inst.secondary_portno
    OR dim.primary_flag <> inst.primary_flag
    OR dim.secondary_flag <> inst.secondary_flag
    OR dim.acq_bank_desc <> inst.acq_bank_desc
    OR dim.isactive <> inst.isactive
    OR dim.crtupd_reason <> inst.crtupd_reason
    OR dim.crtupd_status <> inst.crtupd_status
    OR dim.crtupd_user <> inst.crtupd_user;

-- Updating Previous Records as False
WITH MaxVersions AS (
    SELECT
        acq_bank_code,
        MAX(dim.version) AS max_version
    FROM
        edw_dim.tmp_src_acquiring_bank AS inst
        JOIN edw_dim.dw_dim_acquiring_bank AS dim ON dim.acq_bank_code = inst.acq_bank_code
        AND dim.date_from = inst.crtupd_dt
    GROUP BY
        dim.acq_bank_code
)
UPDATE
    edw_dim.dw_dim_acquiring_bank AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.acq_bank_code = mv.acq_bank_code
    AND dim.version < mv.max_version;

-- Reordering the Dates
WITH next_entries AS (
    SELECT
        acq_bank_code,
        version,
        LEAD(date_from) OVER (
            PARTITION BY acq_bank_code
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.tmp_src_acquiring_bank inst
    JOIN edw_dim.dw_dim_acquiring_bank AS dim ON dim.acq_bank_code = inst.acq_bank_code
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
    edw_dim.dw_dim_acquiring_bank AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.acq_bank_code = next_entries.acq_bank_code
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;
