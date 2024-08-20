WITH
  token_prices AS (
    SELECT
      DATE(minute) AS day,
      AVG(price) AS token_price
    FROM
      prices.usd
    WHERE
      contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
      AND minute >= CURRENT_DATE - INTERVAL '45' DAY
    GROUP BY
      DATE(minute)
  ),
  max_price AS (
    SELECT
      day,
      token_price
    FROM
      token_prices
    ORDER BY
      token_price DESC
    LIMIT
      1
  ),
  min_price_after_max AS (
    SELECT
      MIN(token_price) AS min_price
    FROM
      token_prices
    WHERE
      day > (
        SELECT
          day
        FROM
          max_price
      )
  )
SELECT
  (
    (
      SELECT
        token_price
      FROM
        max_price
    ) - (
      SELECT
        min_price
      FROM
        min_price_after_max
    )
  ) / (
    SELECT
      token_price
    FROM
      max_price
  ) * 100 AS max_drawdown;