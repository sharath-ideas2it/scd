BEGIN;
--insert new records which isn't exist in the dimension table
INSERT INTO edw_dim.dw_dim_monthly_kpi_target (
        kpi_name ,
        "month" ,
        "year" ,
        category ,
        target ,
        "version" ,
        lastversion ,
        date_from ,
        date_to
    )
    SELECT
        inst.src_kpi_name ,
        inst.src_month ,
        inst.src_year ,
        inst.src_category ,
        inst.src_target ,
        1 as "version",
        true as lastversion,
        now(),
        '2200-01-01 00:00:00.000'::timestamp as data_to
    FROM
        edw_dim.tmp_src_monthly_kpi_target AS inst
    LEFT JOIN
        edw_dim.dw_dim_monthly_kpi_target AS dim
    ON
        dim.kpi_name = inst.src_kpi_name
        and dim.month = inst.src_month
        and dim.year = inst.src_year
        and dim.category = inst.src_category
    WHERE
        dim.kpi_name IS NULL;


--insert the update records as new records with last version has true and increment in version
INSERT INTO edw_dim.dw_dim_monthly_kpi_target (
        kpi_name ,
        "month" ,
        "year" ,
        category ,
        target ,
        "version" ,
        lastversion ,
        date_from ,
        date_to
    )
    SELECT
        inst.src_kpi_name ,
        inst.src_month ,
        inst.src_year ,
        inst.src_category ,
        inst.src_target ,
        "version"+1 as "version",
        true as lastversion,
        now(),
        '2200-01-01 00:00:00.000'::timestamp as data_to
    FROM
        edw_dim.tmp_src_monthly_kpi_target AS inst
    LEFT JOIN
        edw_dim.dw_dim_monthly_kpi_target AS dim
    ON
        dim.kpi_name = inst.src_kpi_name
        and dim.month = inst.src_month
        and dim.year = inst.src_year
        and dim.category = inst.src_category
    WHERE
        dim.target <> inst.src_target AND dim.lastversion = true;


--update existing record last version and date_to column
UPDATE edw_dim.dw_dim_monthly_kpi_target AS dim
SET date_to = src_created_on,
    lastversion = false
FROM edw_dim.tmp_src_monthly_kpi_target AS upt
WHERE
    dim.kpi_name = upt.src_kpi_name
    and dim.month = upt.src_month
    and dim.year = upt.src_year
    and dim.category = upt.src_category
    and dim.date_from <= CURRENT_DATE
    AND dim."version" = (SELECT "version" FROM (
    SELECT
        dim."version" ,
        ROW_NUMBER() OVER (PARTITION BY dim.kpi_name, dim."month", dim."year",dim.category  ORDER BY dim.version DESC) AS rn
    FROM edw_dim.dw_dim_monthly_kpi_target AS dim
    INNER JOIN edw_dim.tmp_src_monthly_kpi_target AS upt
    ON  dim.kpi_name = upt.src_kpi_name
        and dim.month = upt.src_month
        and dim.year = upt.src_year
        and dim.category = upt.src_category
) ranker WHERE rn = 2);
     
COMMIT;

--Changed

BEGIN;

-- Insert new records where the (kpi_name, month, year, category) does not already exist in the dimension table
INSERT INTO edw_dim.dw_dim_monthly_kpi_target (
    kpi_name,
    "month",
    "year",
    category,
    target,
    "version",
    lastversion,
    date_from,
    date_to
)
SELECT
    inst.src_kpi_name,
    inst.src_month,
    inst.src_year,
    inst.src_category,
    inst.src_target,
    1 as  "version",
    true AS lastversion,
    now() AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_monthly_kpi_target AS inst
LEFT JOIN
    edw_dim.dw_dim_monthly_kpi_target AS dim
ON
    dim.kpi_name = inst.src_kpi_name
    AND dim.month = inst.src_month
    AND dim.year = inst.src_year
    AND dim.category = inst.src_category
WHERE
    dim.kpi_name IS NULL;

-- Insert updated records with a new version number and set lastversion = true
INSERT INTO edw_dim.dw_dim_monthly_kpi_target (
    kpi_name,
    "month",
    "year",
    category,
    target,
    "version",
    date_from,
    date_to
)
SELECT
    inst.src_kpi_name,
    inst.src_month,
    inst.src_year,
    inst.src_category,
    inst.src_target,
    dim."version" + ROW_NUMBER() OVER (
        PARTITION BY inst.src_kpi_name, inst.src_month, inst.src_year, inst.src_category
        ORDER BY inst.src_created_on
    ) AS "version",
    now() AS date_from,
    '2200-01-01 00:00:00.000'::timestamp AS date_to
FROM
    edw_dim.tmp_src_monthly_kpi_target AS inst
LEFT JOIN
    edw_dim.dw_dim_monthly_kpi_target AS dim
ON
    dim.kpi_name = inst.src_kpi_name
    AND dim.month = inst.src_month
    AND dim.year = inst.src_year
    AND dim.category = inst.src_category
    AND dim.lastversion = true
WHERE
    dim.target <> inst.src_target;

-- Update existing records to set lastversion to false and update the date_to field
WITH MaxVersions AS (
    SELECT
        kpi_name,
        "month",
        "year",
        category,
        MAX(dim."version") AS max_version
    FROM
        edw_dim.dw_dim_monthly_kpi_target AS dim
    INNER JOIN edw_dim.tmp_src_monthly_kpi_target AS upt
        ON dim.kpi_name = upt.src_kpi_name
        AND dim.month = upt.src_month
        AND dim.year = upt.src_year
        AND dim.category = upt.src_category
    GROUP BY
        dim.kpi_name, dim.month, dim.year, dim.category
)
UPDATE edw_dim.dw_dim_monthly_kpi_target AS dim
SET
    lastversion = false
FROM
    MaxVersions mv
WHERE
    dim.kpi_name = mv.kpi_name
    AND dim.month = mv.month
    AND dim.year = mv.year
    AND dim.category = mv.category
    AND dim."version" = mv.max_version
    AND dim.lastversion = true;

-- Reordering the date_to field for records
WITH next_entries AS (
  SELECT
    kpi_name,
    "month",
    "year",
    category,
    "version",
    LEAD(date_from) OVER (
      PARTITION BY kpi_name, "month", "year", category
      ORDER BY "version"
    ) AS next_date_from
  FROM
    edw_dim.tmp_src_monthly_kpi_target inst
  JOIN edw_dim.tmp_src_monthly_kpi_target AS dim
        ON dim.kpi_name = inst.src_kpi_name
        AND dim.month = inst.src_month
        AND dim.year = inst.src_year
        AND dim.category = inst.src_category
        AND dim.date_from = inst.created_on
)
UPDATE edw_dim.dw_dim_monthly_kpi_target AS dim
SET
  date_to = next_entries.next_date_from
FROM
  next_entries
WHERE
  dim.kpi_name = next_entries.kpi_name
  AND dim.month = next_entries.month
  AND dim.year = next_entries.year
  AND dim.category = next_entries.category
  AND dim.version = next_entries.version
  AND next_entries.next_date_from IS NOT NULL;

COMMIT;
