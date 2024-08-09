BEGIN;
--insert new records which isn't exist in the dimension table
INSERT INTO edw_dim.dw_dim_chain_category_owner (
    category ,
    owner_name ,
    "version",
    lastversion ,
    date_from ,
    date_to
    )
    SELECT
        inst.src_category ,
        inst.src_owner_name ,
        1 as "version",
        true as lastversion,
        now(),
        '2200-01-01 00:00:00.000'::timestamp as data_to
    FROM
        edw_dim.tmp_src_chain_category_owner AS inst
    LEFT JOIN
        edw_dim.dw_dim_chain_category_owner AS dim
    ON
        dim.category = inst.src_category
    WHERE
        dim.category IS NULL;




--insert the update records as new records with last version has true and increment in version
INSERT INTO edw_dim.dw_dim_chain_category_owner (
    category ,
    owner_name ,
    "version",
    lastversion ,
    date_from ,
    date_to
    )
    SELECT
        inst.src_category ,
        inst.src_owner_name ,
        "version"+1 as "version",
        true as lastversion,
        now(),
        '2200-01-01 00:00:00.000'::timestamp as data_to
    FROM
        edw_dim.tmp_src_chain_category_owner AS inst
    LEFT JOIN
        edw_dim.dw_dim_chain_category_owner AS dim
    ON
        dim.category = inst.src_category
    WHERE
        dim.owner_name <> inst.src_owner_name AND dim.lastversion = true;


--update existing record last version and date_to column
UPDATE edw_dim.dw_dim_chain_category_owner AS dim
SET date_to = src_created_on,
    lastversion = false
FROM edw_dim.tmp_src_chain_category_owner AS upt
WHERE
    dim.category = upt.src_category
    and dim.date_from::date <= CURRENT_DATE
    AND dim."version" = (
   
    SELECT "version" FROM (
    SELECT
        dim."version",
        ROW_NUMBER() OVER (PARTITION BY dim.category ORDER BY dim.version DESC) AS rn
    FROM edw_dim.dw_dim_chain_category_owner AS dim
    INNER JOIN edw_dim.tmp_src_chain_category_owner AS upt
        ON dim.category = upt.src_category
       
   
    ) ranker WHERE rn = 2
);
     
COMMIT;

--Changed

BEGIN;

-- Insert new records where the (category) does not already exist in the dimension table
INSERT INTO edw_dim.dw_dim_chain_category_owner (
    category,
    owner_name,
    date_from,
    date_to
)
SELECT
    inst.src_category,
    inst.src_owner_name,
    now() AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_chain_category_owner AS inst
LEFT JOIN
    edw_dim.dw_dim_chain_category_owner AS dim
ON
    dim.category = inst.src_category
WHERE
    dim.category IS NULL;

-- Insert updated records with a new version number and set lastversion = true
INSERT INTO edw_dim.dw_dim_chain_category_owner (
    category,
    owner_name,
    "version",
    date_from,
    date_to
)
SELECT
    inst.src_category,
    inst.src_owner_name,
    dim."version" + ROW_NUMBER() OVER (
        PARTITION BY inst.src_category
        ORDER BY inst.src_created_on
    ) AS "version",
    now() AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_chain_category_owner AS inst
LEFT JOIN
    edw_dim.dw_dim_chain_category_owner AS dim
ON
    dim.category = inst.src_category
    AND dim.lastversion = true
WHERE
    dim.owner_name <> inst.src_owner_name;

-- Update the existing records: set lastversion to false and update the date_to field
WITH MaxVersions AS (
    SELECT
        category,
        MAX(dim."version") AS max_version
    FROM
        edw_dim.dw_dim_chain_category_owner AS dim
    INNER JOIN edw_dim.tmp_src_chain_category_owner AS upt
        ON dim.category = upt.src_category
    GROUP BY
        dim.category
)
UPDATE edw_dim.dw_dim_chain_category_owner AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.category = mv.category
    AND dim."version" = mv.max_version
    AND dim.lastversion = true;

-- Reordering the date_to field for records
WITH next_entries AS (
  SELECT
    category,
    "version",
    LEAD(date_from) OVER (
      PARTITION BY category
      ORDER BY "version"
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_chain_category_owner
  JOIN edw_dim.tmp_src_chain_category_owner AS upt
        ON dim.category = upt.src_category AND 
        dim.date_from = inst.created_date
)
UPDATE edw_dim.dw_dim_chain_category_owner AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.category = next_entries.category
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;

COMMIT;
