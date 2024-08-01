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
  token_holders_with_balance AS (
    SELECT
      b.day AS "Date",
      COUNT(address) AS "Holders with Balance"
    FROM
      token_balance_all_days AS b
    WHERE
      balance > 0
    GROUP BY
      b.day
  ),
  uni_holders_with_balance AS (
    SELECT
      b.day AS "Date",
      COUNT(address) AS "Holders with Balance"
    FROM
      uni_balance_all_days AS b
    WHERE
      balance > 0
    GROUP BY
      b.day
  ),
  link_holders_with_balance AS (
    SELECT
      b.day AS "Date",
      COUNT(address) AS "Holders with Balance"
    FROM
      link_balance_all_days AS b
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
SELECT
  COALESCE(hwb_token."Date", hwb_uni."Date", hwb_link."Date") AS "Date",
  COALESCE(hwb_token."Holders with Balance", 0) AS "Token Holders with Balance",
  COALESCE(hwb_uni."Holders with Balance", 0) AS "UNI Holders with Balance",
  COALESCE(hwb_link."Holders with Balance", 0) AS "LINK Holders with Balance"
FROM
  token_holders_with_balance hwb_token
  FULL JOIN uni_holders_with_balance hwb_uni ON hwb_token."Date" = hwb_uni."Date"
  FULL JOIN link_holders_with_balance hwb_link ON hwb_token."Date" = hwb_link."Date"
where
  hwb_token."Holders with Balance" > 0
ORDER BY
  COALESCE(hwb_token."Date", hwb_uni."Date", hwb_link."Date")