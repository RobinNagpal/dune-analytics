WITH
  price_data AS (
    SELECT
      median_price,
      hour
    FROM
      dex.prices
    WHERE
      contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
  ),
ranked_prices AS (
    SELECT
        median_price,
        ROW_NUMBER() OVER (ORDER BY median_price) AS row_num,
        COUNT(*) OVER () AS total_rows
    FROM
        price_data
)
SELECT
    AVG(median_price) AS median_dex_price
FROM
    ranked_prices
WHERE
    row_num IN (FLOOR((total_rows + 1) / 2.0), CEIL((total_rows + 1) / 2.0)); --dealing with even and odd no. of rows to get the middle row