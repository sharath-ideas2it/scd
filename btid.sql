-- SCD Handler for Insert Operation: btid
BEGIN;

-- Inserting New Records
INSERT INTO edw_dim.dw_dim_btid (
    btid,
    bank_merchant_id,
    hostindex,
    cardtype,
    panlow,
    panhigh,
    panlenrange,
    amtfloorlimit,
    cardlabel,
    btid_issuedate,
    btid_key_exchanged,
    btid_key_exchanged_on,
    mcc_code,
    category,
    crtupd_status,
    crtupd_user,
    refund_allowed,
    date_from,
    date_to
)
SELECT
    inst.btid,    
    inst.bank_merchant_id,
    inst.hostindex,
    inst.cardtype,
    inst.panlow,
    inst.panhigh,
    inst.panlenrange,
    inst.amtfloorlimit,
    inst.cardlabel,
    inst.btid_issuedate,
    inst.btid_key_exchanged,
    inst.btid_key_exchanged_on,
    inst.mcc_code,
    inst.category,
    inst.crtupd_status,
    inst.crtupd_user,
    inst.refund_allowed,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_bank_terminal_lookup AS inst
    LEFT JOIN edw_dim.dw_dim_btid AS dim ON dim.btid = inst.btid
    AND dim.bank_merchant_id = inst.bank_merchant_id
    AND dim.panlow = inst.panlow
WHERE
    dim.btid IS NULL;

-- Inserting Modified Records
INSERT INTO edw_dim.dw_dim_btid (
    btid,
    bank_merchant_id,
    hostindex,
    cardtype,
    panlow,
    panhigh,
    panlenrange,
    amtfloorlimit,
    cardlabel,
    btid_issuedate,
    btid_key_exchanged,
    btid_key_exchanged_on,
    mcc_code,
    category,
    crtupd_status,
    crtupd_user,
    refund_allowed,
    version,
    date_from,
    date_to
)
SELECT
    inst.btid,    
    inst.bank_merchant_id,
    inst.hostindex,
    inst.cardtype,
    inst.panlow,
    inst.panhigh,
    inst.panlenrange,
    inst.amtfloorlimit,
    inst.cardlabel,
    inst.btid_issuedate,
    inst.btid_key_exchanged,
    inst.btid_key_exchanged_on,
    inst.mcc_code,
    inst.category,
    inst.crtupd_status,
    inst.crtupd_user,
    inst.refund_allowed,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.btid, inst.bank_merchant_id, inst.panlow
        ORDER BY inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_bank_terminal_lookup AS inst
    LEFT JOIN edw_dim.dw_dim_btid AS dim ON dim.btid = inst.btid
    AND dim.bank_merchant_id = inst.bank_merchant_id
    AND dim.panlow = inst.panlow
    AND dim.lastversion = true
WHERE
    dim.hostindex <> inst.hostindex
    OR dim.cardtype <> inst.cardtype
    OR dim.panhigh <> inst.panhigh
    OR dim.panlenrange <> inst.panlenrange
    OR dim.amtfloorlimit <> inst.amtfloorlimit
    OR dim.cardlabel <> inst.cardlabel
    OR dim.btid_issuedate <> inst.btid_issuedate
    OR dim.btid_key_exchanged <> inst.btid_key_exchanged
    OR dim.btid_key_exchanged_on <> inst.btid_key_exchanged_on
    OR dim.mcc_code <> inst.mcc_code
    OR dim.category <> inst.category
    OR dim.crtupd_status <> inst.crtupd_status
    OR dim.crtupd_user <> inst.crtupd_user
    OR dim.refund_allowed <> inst.refund_allowed;

-- Updating Previous Records as False and Determining Maximum Version Number for Each btid
WITH MaxVersions AS (
    SELECT
        btid,
        bank_merchant_id,
        panlow,
        MAX(dim.version) AS max_version
    FROM
        edw_dim.tmp_src_bank_terminal_lookup AS inst
        JOIN edw_dim.dw_dim_btid AS dim ON dim.btid = inst.btid
        AND dim.bank_merchant_id = inst.bank_merchant_id
        AND dim.panlow = inst.panlow
        AND dim.date_from = inst.crtupd_dt
    GROUP BY
        dim.btid, dim.bank_merchant_id, dim.panlow
)
UPDATE
    edw_dim.dw_dim_btid AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.btid = mv.btid
    AND dim.bank_merchant_id = mv.bank_merchant_id
    AND dim.panlow = mv.panlow
    AND dim.version < mv.max_version;

-- Reordering the Dates
WITH next_entries AS (
    SELECT
        btid,
        bank_merchant_id,
        panlow,
        version,
        LEAD(date_from) OVER (
            PARTITION BY btid, bank_merchant_id, panlow
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.tmp_src_bank_terminal_lookup AS inst
        JOIN edw_dim.dw_dim_btid AS dim ON dim.btid = inst.btid
        AND dim.bank_merchant_id = inst.bank_merchant_id
        AND dim.panlow = inst.panlow
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
    edw_dim.dw_dim_btid AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.btid = next_entries.btid
    AND dim.bank_merchant_id = next_entries.bank_merchant_id
    AND dim.panlow = next_entries.panlow
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;



