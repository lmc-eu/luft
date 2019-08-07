SELECT name
FROM `bigquery-public-data.usa_names.usa_1910_2013`
WHERE state = 'TX'
LIMIT 100;

SELECT block_id
FROM `bigquery-public-data.bitcoin_blockchain.transactions`
LIMIT 20;

-- Example of templating
SELECT '{{ BQ_LOCATION }}';
SELECT '{{ BQ_PROJECT_ID }}';
SELECT '{{ DATE_VALID }}';
SELECT '{{ SOURCE_SYSTEM }}';
SELECT '{{ ENV }}';