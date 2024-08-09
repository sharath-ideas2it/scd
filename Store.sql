BEGIN;


-- Insert new records or updated records with new version
INSERT INTO edw_dim.dw_dim_store (
    chain_id,        
    store_code,        
    store_name,        
    mer_store_code,    
    address1,        
    address2,        
    address3,        
    pin_code,        
    city_id,        
    state_id,        
    store_website,    
    billing_gst_number,    
    shipping_gst_number,    
    configuration_date,
    version,
    lastversion,
    date_from,
    date_to,
)
SELECT
    inst.lkp_chain_id,        
    inst.store_code,        
    inst.store_name,        
    inst.mer_store_code,      
    inst.store_adr1,        
    inst.store_adr2,        
    inst.store_adr3,        
    inst.pincode,        
    inst.city_id,        
    inst.state_id,        
    inst.str_website,    
    inst.billing_gst_number,    
    inst.shipping_gst_number,    
    -- inst.crtupd_dt,
    dim.configuration_date,
    COALESCE(dim.max_version, 0) + ROW_NUMBER() OVER (
        PARTITION BY inst.store_code
        ORDER BY inst.crtupd_dt
    ) AS version,
    false AS lastversion,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to,
FROM
    edw_dim.tmp_src_store AS inst
    LEFT JOIN (
        SELECT
            store_code,
            MAX(version) AS max_version
        FROM
            edw_dim.dw_dim_store
        GROUP BY
            store_code
    ) AS dim ON dim.store_code = inst.store_code;


-- Update previous record to lastversion = false
UPDATE
    edw_dim.dw_dim_store AS dim
SET
    lastversion = false
FROM
    edw_dim.tmp_src_store AS upt
WHERE
    dim.store_code = upt.store_code  
    AND dim.lastversion = true;


-- Update the current active record as lastversion = true
UPDATE
    edw_dim.dw_dim_store AS dim
SET
    lastversion = true
FROM
    (
        SELECT
            store_code,
            MAX(version) AS max_version
        FROM
            edw_dim.dw_dim_store
        WHERE
            store_code IN (
                SELECT
                    store_code
                FROM
                    edw_dim.tmp_src_store
            )
        GROUP BY
            store_code
    ) AS latest
WHERE
    dim.store_code = latest.store_code
    AND dim.version = latest.max_version;


-- Update the date_to field in the previous records
WITH next_entries AS (
    SELECT
        store_code,
        version,
        LEAD(date_from) OVER (
            PARTITION BY store_code
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.dw_dim_store
    WHERE
        store_code IN (
            SELECT
                store_code
            FROM
                edw_dim.tmp_src_store
        )
)
UPDATE
    edw_dim.dw_dim_store AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.store_code = next_entries.store_code
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;


COMMIT;

--Changed
BEGIN;

-- Insert new records where store_code does not already exist in the target table
INSERT INTO edw_dim.dw_dim_store (
    chain_id,        
    store_code,        
    store_name,        
    mer_store_code,     
    address1,        
    address2,        
    address3,        
    pin_code,        
    city_id,        
    state_id,        
    store_website,    
    billing_gst_number,    
    shipping_gst_number,    
    configuration_date,
    date_from,
    date_to
)
SELECT
    inst.lkp_chain_id,        
    inst.store_code,        
    inst.store_name,        
    inst.mer_store_code,      
    inst.store_adr1,        
    inst.store_adr2,        
    inst.store_adr3,        
    inst.pincode,        
    inst.city_id,        
    inst.state_id,        
    inst.str_website,    
    inst.billing_gst_number,    
    inst.shipping_gst_number,    
    inst.crtupd_dt AS configuration_date,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_store AS inst
    LEFT JOIN edw_dim.dw_dim_store AS dim ON dim.store_code = inst.store_code
WHERE
    dim.store_code IS NULL;

-- Insert updated records with new version number
INSERT INTO edw_dim.dw_dim_store (
    chain_id,        
    store_code,        
    store_name,        
    mer_store_code,    
    address1,        
    address2,        
    address3,        
    pin_code,        
    city_id,        
    state_id,        
    store_website,    
    billing_gst_number,    
    shipping_gst_number,    
    configuration_date,
    version,
    date_from,
    date_to
)
SELECT
    inst.lkp_chain_id,        
    inst.store_code,        
    inst.store_name,        
    inst.mer_store_code,      
    inst.store_adr1,        
    inst.store_adr2,        
    inst.store_adr3,        
    inst.pincode,        
    inst.city_id,        
    inst.state_id,        
    inst.str_website,    
    inst.billing_gst_number,    
    inst.shipping_gst_number,    
    dim.configuration_date,
    dim.version + ROW_NUMBER() OVER (
        PARTITION BY inst.store_code
        ORDER BY inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_store AS inst
    LEFT JOIN edw_dim.dw_dim_store AS dim ON dim.store_code = inst.store_code
    AND dim.lastversion = true
WHERE
    dim.store_name <> inst.store_name
    OR dim.address1 <> inst.store_adr1
    OR dim.address2 <> inst.store_adr2
    OR dim.address3 <> inst.store_adr3
    OR dim.pin_code <> inst.pincode
    OR dim.city_id <> inst.city_id
    OR dim.state_id <> inst.state_id
    OR dim.store_website <> inst.str_website
    OR dim.billing_gst_number <> inst.billing_gst_number
    OR dim.shipping_gst_number <> inst.shipping_gst_number;

-- Update the previous active record's lastversion to false
WITH MaxVersions AS (
    SELECT
        store_code,
        MAX(dim.version) AS max_version
    FROM
        edw_dim.tmp_src_store AS inst
        JOIN edw_dim.dw_dim_store AS dim ON dim.store_code = inst.store_code
        AND dim.date_from = inst.crtupd_dt
    GROUP BY
        dim.store_code
)
UPDATE
    edw_dim.dw_dim_store AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.store_code = mv.store_code
    AND dim.version < mv.max_version;


-- Update the date_to field in previous records to reflect the start date of the next record
WITH next_entries AS (
    SELECT
        store_code,
        version,
        LEAD(date_from) OVER (
            PARTITION BY store_code
            ORDER BY version
        ) AS next_date_from
    FROM
        edw_dim.tmp_src_store inst
    JOIN edw_dim.dw_dim_store AS dim ON dim.store_code = inst.store_code
        AND dim.date_from = inst.crtupd_dt
)
UPDATE
    edw_dim.dw_dim_store AS dim
SET
    date_to = next_entries.next_date_from
FROM
    next_entries
WHERE
    dim.store_code = next_entries.store_code
    AND dim.version = next_entries.version
    AND next_entries.next_date_from IS NOT NULL;

COMMIT;

