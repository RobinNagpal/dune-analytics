WITH
  token_transfers AS (
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
  token_balances AS (
    SELECT
      DAY,
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      token_transfers
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
      token_transfers
    GROUP BY
      DAY,
      contract_address,
      "from"
  ),
  uni_transfers AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS DAY,
      contract_address,
      "from",
      to,
      value
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{reference_token1}}
      AND "from" <> to
  ),
  uni_balances AS (
    SELECT
      DAY,
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      uni_transfers
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
      uni_transfers
    GROUP BY
      DAY,
      contract_address,
      "from"
  ),
  link_transfers AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS DAY,
      contract_address,
      "from",
      to,
      value
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{reference_token2}}
      AND "from" <> to
  ),
  link_balances AS (
    SELECT
      DAY,
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      link_transfers
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
      link_transfers
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
      token_balances AS t
  ),
  uni_balances_with_gap_days AS (
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
      uni_balances AS t
  ),
  link_balances_with_gap_days AS (
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
      link_balances AS t
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
  uni_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      uni_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  link_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      link_balances_with_gap_days AS b
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
  uni_daily_prices AS (
    SELECT
      er.decimals,
      DATE_TRUNC('day', hour) AS day,
      AVG(dx.median_price) AS price
    FROM
      dex.prices dx
      JOIN tokens.erc20 er ON er.contract_address = {{reference_token1}}
    WHERE
      dx.contract_address = {{reference_token1}}
      AND er.blockchain = '{{chain}}'
    GROUP BY
      er.decimals,
      DATE_TRUNC('day', hour)
  ),
  link_daily_prices AS (
    SELECT
      er.decimals,
      DATE_TRUNC('day', hour) AS day,
      AVG(dx.median_price) AS price
    FROM
      dex.prices dx
      JOIN tokens.erc20 er ON er.contract_address = {{reference_token2}}
    WHERE
      dx.contract_address = {{reference_token2}}
      AND er.blockchain = '{{chain}}'
    GROUP BY
      er.decimals,
      DATE_TRUNC('day', hour)
  ),
  token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN (balance * p.price / POWER(10, p.decimals)) <= 1000 THEN address
        END
      ) AS "Holders with Token Value <= $1000"
    FROM
      token_balance_all_days AS b
      LEFT JOIN token_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  ),
  uni_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN (balance * p.price / POWER(10, p.decimals)) <= 1000 THEN address
        END
      ) AS "Holders with Token Value <= $1000"
    FROM
      uni_balance_all_days AS b
      LEFT JOIN uni_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  ),
  link_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN (balance * p.price / POWER(10, p.decimals)) <= 1000 THEN address
        END
      ) AS "Holders with Token Value <= $1000"
    FROM
      link_balance_all_days AS b
      LEFT JOIN link_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
SELECT
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date") AS "Date",
  COALESCE(htv_token."Holders with Token Value <= $1000", 0) AS "Token Holders with Token Value <= $1000",
  COALESCE(htv_uni."Holders with Token Value <= $1000", 0) AS "UNI Holders with Token Value <= $1000",
  COALESCE(htv_link."Holders with Token Value <= $1000", 0) AS "LINK Holders with Token Value <= $1000"
FROM
  token_holders_with_token_value htv_token
  FULL JOIN uni_holders_with_token_value htv_uni ON htv_token."Date" = htv_uni."Date"
  FULL JOIN link_holders_with_token_value htv_link ON htv_token."Date" = htv_link."Date"
where
  htv_token."Holders with Token Value <= $1000" > 0
ORDER BY
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date");