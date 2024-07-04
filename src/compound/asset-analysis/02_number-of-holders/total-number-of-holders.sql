WITH
  transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          CAST(value AS DECIMAL (38, 0)) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          (-1) * (CAST(value AS DECIMAL (38, 0))) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
      ) AS t
    GROUP BY
      1,
      2,
      3
  ),
  balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(amount) OVER (
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
      transfers AS t
  ),
  days AS (
    SELECT
      DAY
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST(
            DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP
          ),
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
  balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  )
SELECT
  COUNT(address) AS "Holders"
FROM
  balance_all_days AS b
WHERE
  balance > 0

