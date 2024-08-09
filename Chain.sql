---scd handler for insert operation
BEGIN;
INSERT INTO edw_dim.dw_dim_chain (
    chain_code,  
    chain_name,    
    category,    
    address1,    
    address2,    
    address3,    
    pin_code,  
    city_id,      
    state_id,
    chain_website,          
    billing_gst_number,    
    shipping_gst_number,      
    merchant_id,      
    configuration_date,
    version,
    lastversion,
    date_from,
    date_to
)
SELECT
    inst.chain_id,    
    inst.chain_name,      
    inst.chain_category,      
    inst.chain_adr1,      
    inst.chain_adr2,      
    inst.chain_adr3,      
    inst.pincode,        
    inst.city_id,    
    inst.state_id,
    inst.chain_website,    
    inst.billing_gstn,      
    inst.shipping_gstn,    
    inst.merchant_id,    
    -- inst.crtupd_dt,
    dim.configuration_date,
  COALESCE(dim.max_version, 0) + ROW_NUMBER() OVER (
    PARTITION BY inst.chain_id
    ORDER BY
      inst.crtupd_dt  
  ) AS version,
  false AS lastversion,
  inst.crtupd_dt  AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_chain_type AS inst
  LEFT JOIN (
    SELECT
      chain_code ,
      MAX(version) AS max_version
    FROM
      edw_dim.dw_dim_chain
    GROUP BY
      chain_code
  ) AS dim ON dim.chain_code = inst.chain_id;


-- Updating The Previous Record As False
UPDATE
  edw_dim.dw_dim_chain AS dim
SET
  lastversion = false
FROM
  edw_dim.tmp_src_chain_type AS upt
WHERE
  dim.chain_code = upt.chain_id  
  AND dim.lastversion = true;


-- Updating The Current Active Record as True
UPDATE
  edw_dim.dw_dim_chain AS dim
SET
  lastversion = true
FROM
  (
    SELECT
      chain_code,
      MAX(version) AS max_version
    FROM
      edw_dim.dw_dim_chain
    WHERE
      (chain_code) IN (
        SELECT
          chain_id
        FROM
          edw_dim.tmp_src_chain_type
      )
    GROUP BY
      chain_code
  ) AS latest
WHERE
  dim.chain_code = latest.chain_code
  AND dim.version = latest.max_version;
 
-- Updating The Date In Correct Format
WITH next_entries AS (
  SELECT
    chain_code,
    version,
    LEAD(date_from) OVER (
      PARTITION BY chain_code
      ORDER BY
        version
    ) AS next_date_from
  FROM
    edw_dim.dw_dim_chain
  WHERE
    (chain_code) IN (
      SELECT
        chain_id
      FROM
        edw_dim.tmp_src_chain_type
    )
)
UPDATE
  edw_dim.dw_dim_chain AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.chain_code = next_entries.chain_code
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;
COMMIT;


--Changed

BEGIN;
-- Inserting new records
INSERT INTO edw_dim.dw_dim_chain (
    chain_code,  
    chain_name,    
    category,    
    address1,    
    address2,    
    address3,    
    pin_code,  
    city_id,      
    state_id,
    chain_website,          
    billing_gst_number,    
    shipping_gst_number,      
    merchant_id,      
    configuration_date,
    date_from,
    date_to
)
SELECT
    inst.chain_id,    
    inst.chain_name,      
    inst.chain_category,      
    inst.chain_adr1,      
    inst.chain_adr2,      
    inst.chain_adr3,      
    inst.pincode,        
    inst.city_id,    
    inst.state_id,
    inst.chain_website,    
    inst.billing_gstn,      
    inst.shipping_gstn,    
    inst.merchant_id,    
    inst.crtupd_dt AS configuration_date,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_chain_type AS inst
    LEFT JOIN edw_dim.dw_dim_chain AS dim ON dim.chain_code = inst.chain_id
WHERE
    dim.chain_code IS NULL;

-- Inserting modified records
INSERT INTO edw_dim.dw_dim_chain (
    chain_code,  
    chain_name,    
    category,    
    address1,    
    address2,    
    address3,    
    pin_code,  
    city_id,      
    state_id,
    chain_website,          
    billing_gst_number,    
    shipping_gst_number,      
    merchant_id,      
    configuration_date,
    version,
    date_from,
    date_to
)
SELECT
    inst.chain_id,    
    inst.chain_name,      
    inst.chain_category,      
    inst.chain_adr1,      
    inst.chain_adr2,      
    inst.chain_adr3,      
    inst.pincode,        
    inst.city_id,    
    inst.state_id,
    inst.chain_website,    
    inst.billing_gstn,      
    inst.shipping_gstn,    
    inst.merchant_id,    
    dim.configuration_date AS configuration_date,
    dim.version + ROW_NUMBER() OVER (
      PARTITION BY inst.chain_id
      ORDER BY
        inst.crtupd_dt
    ) AS version,
    inst.crtupd_dt AS date_from,
    '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
    edw_dim.tmp_src_chain_type AS inst
    LEFT JOIN edw_dim.dw_dim_chain AS dim ON dim.chain_code = inst.chain_id
    AND dim.lastversion = true
WHERE
    dim.chain_name <> inst.chain_name
    OR dim.category <> inst.chain_category
    OR dim.address1 <> inst.chain_adr1
    OR dim.address2 <> inst.chain_adr2
    OR dim.address3 <> inst.chain_adr3
    OR dim.pin_code <> inst.pincode
    OR dim.city_id <> inst.city_id
    OR dim.state_id <> inst.state_id
    OR dim.chain_website <> inst.chain_website
    OR dim.billing_gst_number <> inst.billing_gstn
    OR dim.shipping_gst_number <> inst.shipping_gstn
    OR dim.merchant_id <> inst.merchant_id;

-- Update previous records to false and determine the maximum version number for each chain_id
WITH MaxVersions AS (
  SELECT
    chain_code,
    MAX(dim."version") AS max_version
  FROM
    edw_dim.tmp_src_chain_type AS inst
    JOIN edw_dim.dw_dim_chain AS dim ON dim.chain_code = inst.chain_id
    AND dim.date_from = inst.crtupd_dt
  GROUP BY
    dim.chain_code
)
UPDATE
  edw_dim.dw_dim_chain AS dim
SET
  lastversion = false
FROM
  MaxVersions mv
WHERE
  dim.chain_code = mv.chain_code
  AND dim.version < mv.max_version;

-- Reordering the dates
WITH next_entries AS (
  SELECT
    chain_code,
    version,
    LEAD(date_from) OVER (
      PARTITION BY chain_code
      ORDER BY
        version
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_chain_type AS inst
    JOIN edw_dim.dw_dim_chain AS dim ON dim.chain_code = inst.chain_id
    AND dim.date_from = inst.crtupd_dt
)
UPDATE
  edw_dim.dw_dim_chain AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.chain_code = next_entries.chain_code
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;

COMMIT;
