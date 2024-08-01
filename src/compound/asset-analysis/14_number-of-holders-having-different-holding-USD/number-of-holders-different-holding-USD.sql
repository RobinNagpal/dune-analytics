WITH
transfers AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS DAY,
      contract_address,
      "from",
      to,
      value
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "from" <> to
  ),
  balances AS (
    SELECT
      DAY,
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      DAY,
      contract_address,
      to
    UNION ALL
    SELECT
      DAY,
      contract_address,
      "from" AS address,
      - SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      DAY,
      contract_address,
      "from"
  ),
  token_balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(balance) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS balance,
      LEAD(DAY, 1, CURRENT_TIMESTAMP) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS next_day
    FROM
      balances AS t
  ),
  days AS (
    SELECT
      DAY
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2021-01-01' AS TIMESTAMP),
          CAST(
            TRY_CAST(
              TRY_CAST(
                TRY_CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP) AS TIMESTAMP
              ) AS TIMESTAMP
            ) AS TIMESTAMP
          ),
          INTERVAL '1' day
        )
      ) AS _u (DAY)
  ),
  token_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      token_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  token_daily_prices AS (
    SELECT
      er.decimals,
      DATE_TRUNC('day', hour) AS day,
      AVG(dx.median_price) AS price
    FROM
      dex.prices dx
      JOIN tokens.erc20 er ON er.contract_address = {{token_address}}
    WHERE
      dx.contract_address = {{token_address}}
      AND er.blockchain = '{{chain}}'
    GROUP BY
      er.decimals,
      DATE_TRUNC('day', hour)
  ),
  token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 0 AND 100 THEN address END) AS "0-100",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 100 AND 1000 THEN address END) AS "100-1000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 1000 AND 10000 THEN address END) AS "1000-10000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 10000 AND 100000 THEN address END) AS "10000-100000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) > 100000 THEN address END) AS "100000+"
    FROM
      token_balance_all_days AS b
      LEFT JOIN token_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
SELECT
  htv_token."Date" AS "Date",
  COALESCE(htv_token."0-100", 0) AS "0-100",
  COALESCE(htv_token."100-1000", 0) AS "100-1000",
  COALESCE(htv_token."1000-10000", 0) AS "1000-10000",
  COALESCE(htv_token."10000-100000", 0) AS "10000-100000",
  COALESCE(htv_token."100000+", 0) AS "100000+"
FROM
  token_holders_with_token_value htv_token
ORDER BY
  1