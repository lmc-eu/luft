-- Create history table
CREATE TABLE IF NOT EXISTS {{ HISTORY_SCHEMA }}.{{ TABLE_NAME }} (
    -------------------------------- PKs -------------------------------------
    {{ PK_DEFINITION_LIST }}{{ ',' if PK_DEFINITION_LIST and PK_DEFINITION_LIST|length else '' }}
    ------------------------------- Tech -------------------------------------
    dw_load_date TIMESTAMP NOT NULL,
    dw_valid_from TIMESTAMP NOT NULL,
    dw_valid_to TIMESTAMP NOT NULL,
    dw_current_flag BOOLEAN NOT NULL,
    dw_gdpr_flag STRING NOT NULL,
    dw_source STRING NOT NULL,
    dw_hash_diff NUMERIC NOT NULL{{ ',' if COLUMN_DEFINITION_LIST and COLUMN_DEFINITION_LIST|length else '' }}
    ------------------------------ Columns -----------------------------------
    {{ COLUMN_DEFINITION_LIST }}
    --------------------------------------------------------------------------
);

-- When this script runs multiple times a day we need to delete this day
DELETE FROM {{ HISTORY_SCHEMA }}.{{ TABLE_NAME}} WHERE dw_valid_from >= timestamp ('{{ DATE_VALID }}');
-- We need to set current flag to False otherwise we will get multiple trues for one PK
UPDATE {{ HISTORY_SCHEMA }}.{{ TABLE_NAME }} SET dw_current_flag = False WHERE dw_current_flag = True;

-- Merge new data into historic table
MERGE INTO {{ HISTORY_SCHEMA }}.{{ TABLE_NAME }} t
    USING (
        SELECT
            -- make sure to add column after Not empty PK
            {{ PK }}{{ ',' if PK and PK|length else '' }}
            -- DW columns
            current_timestamp                                               AS dw_load_date,
            -- New row has alwas same DW_VALID_FROM and DW_VALID_TO
            timestamp('{{ DATE_VALID }}')                                        AS dw_valid_from,
            timestamp('{{ DATE_VALID }}')                                        AS dw_valid_to,
            -- New row is always current
            True                                                            AS dw_current_flag,
            -- Not implemented
            'N'                                                             AS dw_gdpr_flag,
            '{{ SOURCE_SYSTEM }}.{{ SOURCE_SUBSYSTEM }}.{{ TABLE_NAME }}'   AS dw_source,
            -- For finding changes
            FARM_FINGERPRINT(CONCAT({{ HASH_COLUMNS }}))                    AS dw_hash_diff
            {{ ',' if COLUMNS and COLUMNS|length else '' }}{{ COLUMNS }}
            FROM (
                -- We need unique columns
                SELECT DISTINCT
                    {{ PK }}{{ ',' if PK and PK|length else '' }}
                    {{ COLUMNS }}
                FROM {{ STAGE_SCHEMA }}.{{ TABLE_NAME }}
              ) i
    ) s
    -- first we need same PKs
    ON ({{ PK_JOIN }}{{ ' AND' if PK_JOIN and PK_JOIN|length else '' }}
        -- and same hashes because we want to compare only same rows
        t.DW_HASH_DIFF = s.DW_HASH_DIFF
        -- and also need compare only last instance (day) with new data
        AND t.DW_VALID_TO = TIMESTAMP_SUB(s.DW_VALID_TO, INTERVAL 1 DAY))
    -- if new data is not same as last increment then insert data
    WHEN NOT MATCHED THEN
        INSERT (
            {{ PK }}{{ ',' if PK and PK|length else '' }}
            DW_LOAD_DATE,
            DW_VALID_FROM,
            DW_VALID_TO,
            DW_CURRENT_FLAG,
            DW_GDPR_FLAG,
            DW_SOURCE,
            DW_HASH_DIFF
            {{ ',' if COLUMNS and COLUMNS|length else '' }}{{ COLUMNS }}
        )
        VALUES (
            {{ PK }}{{ ',' if PK and PK|length else '' }}
            DW_LOAD_DATE,
            DW_VALID_FROM,
            DW_VALID_TO,
            DW_CURRENT_FLAG,
            DW_GDPR_FLAG,
            DW_SOURCE,
            DW_HASH_DIFF
            {{ ',' if COLUMNS and COLUMNS|length else '' }}{{ COLUMNS }}
        )
    -- else update DW_VALID_TO to last
    WHEN MATCHED THEN
        UPDATE SET t.DW_VALID_TO = s.DW_VALID_TO,
                   t.DW_CURRENT_FLAG = True
;
