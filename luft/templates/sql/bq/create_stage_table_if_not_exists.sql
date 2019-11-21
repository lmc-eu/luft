-- Create stage table
CREATE TABLE IF NOT EXISTS  {{ STAGE_SCHEMA }}.{{ TABLE_NAME }} (
    -------------------------------- PKs -------------------------------------
    {{ PK_DEFINITION_LIST }}{{ ',' if PK_DEFINITION_LIST and PK_DEFINITION_LIST|length and COLUMN_DEFINITION_LIST and COLUMN_DEFINITION_LIST|length else '' }}
    ------------------------------ Columns -----------------------------------
    {{ COLUMN_DEFINITION_LIST }}
    --------------------------------------------------------------------------
);