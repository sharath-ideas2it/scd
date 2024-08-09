-- SCD Handler for Insert Operation: acquirer_pan_info
BEGIN;

-- Inserting New Records
INSERT INTO edw_dim.dw_dim_acquirer_pan_info (
    acq_bank_code,
    panlow,
    panhigh,
    panlenrange,
    cardlabel,
    date_from,
    date_to
)
SELECT
    inst.acq_bank_code,
    inst.panlow,
    inst.panhigh,
    inst.panlenrange,
    inst.cardlabel,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_acquirer_pan_info AS inst
    LEFT JOIN edw_dim.dw_dim_acquirer_pan_info AS dim ON dim.acq_bank_code = inst.acq_bank_code
    AND dim.panlow = inst.panlow
WHERE
    dim.acq_bank_code IS NULL;

-- Inserting Modified Records
INSERT INTO edw_dim.dw_dim_acquirer_pan_info (
    acq_bank_code,
    panlow,
    panhigh,
    panlenrange,
    cardlabel,
    version,
    date_from,
    date_to
)
SELECT
    inst.acq_bank_code,
    inst.panlow,
    inst.panhigh,
    inst.panlenrange,
    inst.cardlabel,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.acq_bank_code, inst.panlow
        ORDER BY inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_acquirer_pan_info AS inst
    LEFT JOIN edw_dim.dw_dim_acquirer_pan_info AS dim ON dim.acq_bank_code = inst.acq_bank_code
    AND dim.panlow = inst.panlow
    AND dim.lastversion = true
WHERE
    dim.panhigh <> inst.panhigh
    OR dim.panlenrange <> inst.panlenrange
    OR dim.cardlabel <> inst.cardlabel;

-- Updating Previous Records as False
WITH MaxVersions AS (
    SELECT
        acq_bank_code,
        panlow,
        MAX(dim.version) AS max_version
    FROM
        edw_dim.tmp_src_acquirer_pan_info AS inst
        JOIN edw_dim.dw_dim_acquirer_pan_info AS dim ON dim.acq_bank_code = inst.acq_bank_code
        AND dim.panlow = inst.panlow
        AND dim.date_from = inst.crtupd_dt
    GROUP BY
        dim.acq_bank_code, dim.panlow
)
UPDATE
    edw_dim.dw_dim_acquirer_pan_info AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.acq_bank_code = mv.acq_bank_code
    AND dim.panlow = mv.panlow
    AND dim.version < mv.max_version;

-- Reordering the Dates
WITH next_entries AS (
    SELECT
        acq_bank_code,
        panlow,
        version,
        LEAD(date_from) OVER (
            PARTITION BY acq_bank_code, panlow
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.tmp_src_acquirer_pan_info inst
    JOIN edw_dim.dw_dim_acquirer_pan_info AS dim ON dim.acq_bank_code = inst.acq_bank_code
        AND dim.panlow = inst.panlow
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
    edw_dim.dw_dim_acquirer_pan_info AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.acq_bank_code = next_entries.acq_bank_code
    AND dim.panlow = next_entries.panlow
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;
