---scd handler for insert operation
BEGIN;
--inserting new records....
INSERT INTO edw_dim.dw_dim_merchant (
  merchant_code, merchant_name, category,
  address1, address2, address3, pin_code,
  city_id, state_id, merchant_website,
  nac, is_reliance_card_enabled, mcc_code,
  do_business_as, configuration_date,
  date_from, date_to
)
SELECT
  inst.mer_id,
  inst.mer_name,
  inst.mer_category,
  inst.mer_adr1,
  inst.mer_adr2,
  inst.mer_adr3,
  inst.pincode,
  inst.city_id,
  inst.state_id,
  inst.mer_website,
  inst.nac,
  inst.is_reliance_card_enabled,
  inst.mcc_code,
  inst.do_business_as,
  inst.crtupd_dt AS configuration_date,
  inst.crtupd_dt AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_merchant AS inst
  LEFT JOIN edw_dim.dw_dim_merchant AS dim ON dim.merchant_code = inst.mer_id
WHERE
  dim.merchant_code IS NULL;


---inserting modified records......
INSERT INTO edw_dim.dw_dim_merchant (
  merchant_code, merchant_name, category,
  address1, address2, address3, pin_code,
  city_id, state_id, merchant_website,
  nac, is_reliance_card_enabled, mcc_code,
  do_business_as, configuration_date,
  version, date_from, date_to
)
SELECT
  inst.mer_id,
  inst.mer_name,
  inst.mer_category,
  inst.mer_adr1,
  inst.mer_adr2,
  inst.mer_adr3,
  inst.pincode,
  inst.city_id,
  inst.state_id,
  inst.mer_website,
  inst.nac,
  inst.is_reliance_card_enabled,
  inst.mcc_code,
  inst.do_business_as,
  dim.configuration_date AS configuration_date,
  dim.version + ROW_NUMBER() OVER (
    PARTITION BY inst.mer_id
    ORDER BY
      inst.crtupd_dt
  ) AS version,
  inst.crtupd_dt AS date_from,
  '2200-01-01 00:00:00.000' :: timestamp AS date_to
FROM
  edw_dim.tmp_src_merchant AS inst
  LEFT JOIN edw_dim.dw_dim_merchant AS dim ON dim.merchant_code = inst.mer_id
  and dim.lastversion = true
WHERE
  dim.merchant_name <> inst.mer_name
  or dim.category <> inst.mer_category
  or dim.address1 <> inst.mer_adr1
  or dim.address2 <> inst.mer_adr2
  or dim.address3 <> inst.mer_adr3
  or dim.pin_code <> inst.pincode
  or dim.city_id <> inst.city_id
  or dim.state_id <> inst.state_id
  or dim.merchant_website <> inst.mer_website
  or dim.nac <> inst.nac
  or dim.is_reliance_card_enabled <> inst.is_reliance_card_enabled
  or dim.mcc_code <> inst.mcc_code
  or dim.do_business_as <> inst.do_business_as;


--Update previous records to false and determine the maximum version number for each merchant_id
WITH MaxVersions AS (
  SELECT
    merchant_code,
    MAX(dim."version") max_version
  FROM
    edw_dim.tmp_src_merchant AS inst
    JOIN edw_dim.dw_dim_merchant AS dim on dim.merchant_code = inst.mer_id
    AND dim.date_from = inst.crtupd_dt
  GROUP BY
    dim.merchant_code
)
UPDATE
  edw_dim.dw_dim_merchant dim
SET
  lastversion = false
FROM
  MaxVersions mv
WHERE
  dim.merchant_code = mv.merchant_code
  AND dim.version < mv.max_version;


-- Reordering the date's
WITH next_entries AS (
  SELECT
    merchant_code,
    version,
    LEAD(date_from) OVER (
      PARTITION BY merchant_code
      ORDER BY
        version
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_merchant AS inst
    JOIN edw_dim.dw_dim_merchant AS dim on dim.merchant_code = inst.mer_id
    AND dim.date_from = inst.crtupd_dt
)
UPDATE
  edw_dim.dw_dim_merchant AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.merchant_code = next_entries.merchant_code
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;
COMMIT;