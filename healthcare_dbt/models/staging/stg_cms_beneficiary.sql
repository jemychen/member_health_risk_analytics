-- models/staging/stg_cms_beneficiary.sql
-- Silver Layer: Clean and standardize raw beneficiary data

WITH source AS (
    SELECT * FROM bronze.cms_beneficiary_raw
),

cleaned AS (
    SELECT
        -- Identifiers
        BENE_ID                                    AS beneficiary_id,
        
        -- Demographics
        STATE_CODE                                 AS state_code,
        COUNTY_CD                                  AS county_code,
        ZIP_CD                                     AS zip_code,
        SEX_IDENT_CD                               AS gender_code,
        CASE 
            WHEN SEX_IDENT_CD = 1 THEN 'Male'
            WHEN SEX_IDENT_CD = 2 THEN 'Female'
            ELSE 'Unknown'
        END                                        AS gender,
        BENE_RACE_CD                               AS race_code,
        CASE
            WHEN BENE_RACE_CD = 1 THEN 'White'
            WHEN BENE_RACE_CD = 2 THEN 'Black'
            WHEN BENE_RACE_CD = 3 THEN 'Other'
            WHEN BENE_RACE_CD = 5 THEN 'Hispanic'
            ELSE 'Unknown'
        END                                        AS race,

        -- Dates
        TO_DATE(BENE_BIRTH_DT, 'dd-MMM-yyyy')     AS birth_date,
        TO_DATE(BENE_DEATH_DT, 'dd-MMM-yyyy')     AS death_date,

        -- Enrollment
        BENE_ENROLLMT_REF_YR                       AS enrollment_year,
        BENE_HI_CVRAGE_TOT_MONS                    AS part_a_months,
        BENE_SMI_CVRAGE_TOT_MONS                   AS part_b_months,
        BENE_HMO_CVRAGE_TOT_MONS                   AS hmo_months,
        BENE_STATE_BUYIN_TOT_MONS                  AS state_buyin_months,

        -- Risk Indicators
        ESRD_IND                                   AS esrd_indicator,
        CASE
            WHEN ESRD_IND = 'Y' THEN TRUE
            ELSE FALSE
        END                                        AS has_esrd,

        -- Metadata
        CURRENT_TIMESTAMP()                        AS loaded_at

    FROM source
    WHERE BENE_ID IS NOT NULL
)

SELECT * FROM cleaned