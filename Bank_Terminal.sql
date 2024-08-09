BEGIN;

-- Insert new records where the combination of utid, acq_bank_code, btid, and inno_scheme_model_code does not already exist in the target table
INSERT INTO dw_dim_bank_terminal (
    utid,
    acq_bank_code,
    btid,
    bank_merchant_id,
    saletype,
    btid_state,
    crtupd_status,
    crtupd_dt,
    crtupd_user,
    bank_stan_no,
    batch_srl_no,
    invoice_no,
    inno_scheme_model_code,
    on_us_off_us,
    date_from,
    date_to,
    general_purpose_utid
)
SELECT 
    inst.src_utid AS utid,
    inst.src_acq_bank_code AS acq_bank_code,
    inst.src_btid AS btid,
    inst.src_bank_merchant_id AS bank_merchant_id,
    inst.src_saletype AS saletype,
    inst.src_btid_state AS btid_state,
    inst.src_crtupd_status AS crtupd_status,
    inst.src_crtupd_dt AS crtupd_dt,
    inst.src_crtupd_user AS crtupd_user,
    inst.src_bank_stan_no AS bank_stan_no,
    inst.src_batch_srl_no AS batch_srl_no,
    inst.src_invoice_no AS invoice_no,
    inst.src_inno_scheme_model_code AS inno_scheme_model_code,
    inst.src_on_us_off_us AS on_us_off_us,
    inst.src_crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to,
    inst.src_utid AS general_purpose_utid
FROM 
    tmp_src_bank_terminal AS inst
    LEFT JOIN dw_dim_bank_terminal AS dim
    ON dim.utid = inst.src_utid
    AND dim.acq_bank_code = inst.src_acq_bank_code
    AND dim.btid = inst.src_btid
    AND dim.inno_scheme_model_code = inst.src_inno_scheme_model_code
WHERE 
    dim.utid IS NULL;

-- Insert updated records with new version number
INSERT INTO dw_dim_bank_terminal (
    utid,
    acq_bank_code,
    btid,
    bank_merchant_id,
    saletype,
    btid_state,
    crtupd_status,
    crtupd_dt,
    crtupd_user,
    bank_stan_no,
    batch_srl_no,
    invoice_no,
    inno_scheme_model_code,
    on_us_off_us,
    date_from,
    date_to,
    version,
    general_purpose_utid
)
SELECT 
    inst.src_utid AS utid,
    inst.src_acq_bank_code AS acq_bank_code,
    inst.src_btid AS btid,
    inst.src_bank_merchant_id AS bank_merchant_id,
    inst.src_saletype AS saletype,
    inst.src_btid_state AS btid_state,
    inst.src_crtupd_status AS crtupd_status,
    inst.src_crtupd_dt AS crtupd_dt,
    inst.src_crtupd_user AS crtupd_user,
    inst.src_bank_stan_no AS bank_stan_no,
    inst.src_batch_srl_no AS batch_srl_no,
    inst.src_invoice_no AS invoice_no,
    inst.src_inno_scheme_model_code AS inno_scheme_model_code,
    inst.src_on_us_off_us AS on_us_off_us,
    inst.src_crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.src_utid, inst.src_acq_bank_code, inst.src_btid, inst.src_inno_scheme_model_code
        ORDER BY inst.src_crtupd_dt  
    ) AS version,
    inst.src_utid AS general_purpose_utid
FROM 
    tmp_src_bank_terminal AS inst
    LEFT JOIN dw_dim_bank_terminal AS dim 
    ON dim.utid = inst.src_utid
    AND dim.acq_bank_code = inst.src_acq_bank_code
    AND dim.btid = inst.src_btid
    AND dim.inno_scheme_model_code = inst.src_inno_scheme_model_code
    AND dim.lastversion = true
WHERE   dim.bank_merchant_id <> inst.src_bank_merchant_id
        OR dim.saletype <> inst.src_saletype
        OR dim.btid_state <> inst.src_btid_state
        OR dim.crtupd_status <> inst.src_crtupd_status
        OR dim.crtupd_dt <> inst.src_crtupd_dt
        OR dim.crtupd_user <> inst.src_crtupd_user
        OR dim.bank_stan_no <> inst.src_bank_stan_no
        OR dim.batch_srl_no <> inst.src_batch_srl_no
        OR dim.invoice_no <> inst.src_invoice_no
        OR dim.on_us_off_us <> inst.src_on_us_off_us

-- Update the previous active record's lastversion to false
WITH MaxVersions AS (
    SELECT
        utid, 
        acq_bank_code, 
        btid, 
        inno_scheme_model_code,
        MAX(version) AS max_version
    FROM 
        dw_dim_bank_terminal inst
    JOIN dw_dim_bank_terminal AS dim 
        ON dim.utid = inst.src_utid
        AND dim.acq_bank_code = inst.src_acq_bank_code
        AND dim.btid = inst.src_btid
        AND dim.inno_scheme_model_code = inst.src_inno_scheme_model_code
        and dim.date_from = inst.crtupd_dt
    GROUP BY
        utid, acq_bank_code, btid, inno_scheme_model_code
)
UPDATE 
    dw_dim_bank_terminal AS dim 
SET 
    lastversion = false 
FROM 
    MaxVersions mv
WHERE 
    dim.utid = mv.utid 
    AND dim.acq_bank_code = mv.acq_bank_code
    AND dim.btid = mv.btid
    AND dim.inno_scheme_model_code = mv.inno_scheme_model_code
    AND dim.version < mv.max_version;

-- Update the date_to field in previous records to reflect the start date of the next record
WITH next_entries AS (
    SELECT 
        utid, 
        acq_bank_code,
        btid,
        inno_scheme_model_code,
        version,
        LEAD(date_from) OVER (
            PARTITION BY utid, acq_bank_code, btid, inno_scheme_model_code
            ORDER BY version
        ) AS next_date_from 
    FROM 
        tmp_src_bank_terminal inst
    JOIN dw_dim_bank_terminal AS dim 
        ON dim.utid = inst.src_utid
        AND dim.acq_bank_code = inst.src_acq_bank_code
        AND dim.btid = inst.src_btid
        AND dim.inno_scheme_model_code = inst.src_inno_scheme_model_code
        and dim.date_from = inst.crtupd_dt
) 
UPDATE 
    dw_dim_bank_terminal AS dim 
SET 
    date_to = next_entries.next_date_from 
FROM 
    next_entries 
WHERE 
    dim.utid = next_entries.utid 
    AND dim.acq_bank_code = next_entries.acq_bank_code
    AND dim.btid = next_entries.btid
    AND dim.inno_scheme_model_code = next_entries.inno_scheme_model_code
    AND dim.version = next_entries.version 
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;
