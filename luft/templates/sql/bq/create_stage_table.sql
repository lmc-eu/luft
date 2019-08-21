-- Create stage table
CREATE OR REPLACE TABLE {{ STAGE_SCHEMA }}.{{ TABLE_NAME }} (
    -------------------------------- PKs -------------------------------------
    {{ PK_DEFINITION_LIST }}{{ ',' if PK_DEFINITION_LIST and PK_DEFINITION_LIST|length else '' }}
    ------------------------------ Columns -----------------------------------
    {{ COLUMN_DEFINITION_LIST }}
    --------------------------------------------------------------------------
);