-- models/marts/risk/fct_member_risk.sql
-- Gold Layer: Member risk scoring model

WITH beneficiary AS (
    SELECT * FROM {{ ref('stg_cms_beneficiary') }}
),

risk_scored AS (
    SELECT
        beneficiary_id,
        gender,
        race,
        state_code,
        zip_code,
        birth_date,
        death_date,
        enrollment_year,
        part_a_months,
        part_b_months,
        hmo_months,
        has_esrd,

        -- Age calculation
        DATEDIFF(CURRENT_DATE(), birth_date) / 365  AS age,

        -- Risk scoring logic
        CASE
            WHEN has_esrd = TRUE                     THEN 3
            ELSE 0
        END +
        CASE
            WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 75  THEN 3
            WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 65  THEN 2
            WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 55  THEN 1
            ELSE 0
        END +
        CASE
            WHEN part_a_months < 6               THEN 2
            WHEN part_a_months < 9               THEN 1
            ELSE 0
        END +
        CASE
            WHEN part_b_months < 6               THEN 2
            WHEN part_b_months < 9               THEN 1
            ELSE 0
        END                                          AS risk_score,

        -- Risk tier
        CASE
            WHEN (
                CASE WHEN has_esrd = TRUE THEN 3 ELSE 0 END +
                CASE
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 75 THEN 3
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 65 THEN 2
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 55 THEN 1
                    ELSE 0
                END +
                CASE WHEN part_a_months < 6 THEN 2 WHEN part_a_months < 9 THEN 1 ELSE 0 END +
                CASE WHEN part_b_months < 6 THEN 2 WHEN part_b_months < 9 THEN 1 ELSE 0 END
            ) >= 6                                   THEN 'High Risk'
            WHEN (
                CASE WHEN has_esrd = TRUE THEN 3 ELSE 0 END +
                CASE
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 75 THEN 3
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 65 THEN 2
                    WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 55 THEN 1
                    ELSE 0
                END +
                CASE WHEN part_a_months < 6 THEN 2 WHEN part_a_months < 9 THEN 1 ELSE 0 END +
                CASE WHEN part_b_months < 6 THEN 2 WHEN part_b_months < 9 THEN 1 ELSE 0 END
            ) >= 3                                   THEN 'Medium Risk'
            ELSE                                          'Low Risk'
        END                                          AS risk_tier,

        -- Metadata
        CURRENT_TIMESTAMP()                          AS scored_at

    FROM beneficiary
    WHERE death_date IS NULL  -- Only active members
)

SELECT * FROM risk_scored