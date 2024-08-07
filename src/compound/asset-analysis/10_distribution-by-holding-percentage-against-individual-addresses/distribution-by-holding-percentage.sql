WITH
  decimals_info_token AS (
    SELECT
      decimals
    FROM
      tokens.erc20
    WHERE
      contract_address = {{token_address}}
  ),
  token_total_supply AS (
    SELECT
      sum(tokens / POWER(10, d.decimals)) as total_supply
    FROM
      (
        SELECT
          wallet,
          sum(amount) AS tokens
        FROM
          (
            SELECT
              "to" AS wallet,
              contract_address,
              SUM(cast(value as double)) AS amount
            FROM
              erc20_{{chain}}.evt_Transfer tr
            WHERE
              contract_address = {{token_address}}
            GROUP BY
              1,
              2
            UNION ALL
            SELECT
              "from" AS wallet,
              contract_address,
              - SUM(cast(value as double)) AS amount
            FROM
              erc20_{{chain}}.evt_Transfer tr
            WHERE
              contract_address = {{token_address}}
            GROUP BY
              1,
              2
          ) t
        GROUP BY
          1
      ) a
      CROSS JOIN decimals_info_token d
    WHERE
      tokens > 0
  ),
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
          TRY_CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP),
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
      SUM(
        balance / TRY_CAST(POWER(10, di.decimals) AS DOUBLE)
      ) AS balance
    FROM
      token_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
      CROSS JOIN decimals_info_token di
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  dex_cex_addresses AS (
    SELECT
      address AS address
    FROM
      cex.addresses
    WHERE
      blockchain = '{{chain}}'
    UNION ALL
    SELECT
      address
    FROM
      (
        SELECT
          address AS address
        FROM
          dex.addresses
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
        UNION ALL
        SELECT
          project_contract_address AS address
        FROM
          dex.trades
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
      )
  ),
  token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN b.balance > 0
          AND b.balance <= ts.total_supply * 0.000000025 THEN b.address
        END
      ) AS "0-0.0000025%",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.000000025
          AND b.balance <= ts.total_supply * 0.00000025 THEN b.address
        END
      ) AS "0.0000025-0.000025%",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.00000025
          AND b.balance <= ts.total_supply * 0.000005 THEN b.address
        END
      ) AS "0.000025-0.0005%",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.000005
          AND b.balance <= ts.total_supply * 0.00005 THEN b.address
        END
      ) AS "0.0005-0.005%",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.00005 THEN b.address
        END
      ) AS ">.005%"
    FROM
      token_balance_all_days AS b
      LEFT JOIN contracts.contract_mapping c ON address = c.contract_address
      CROSS JOIN token_total_supply ts
    WHERE
      address NOT IN (
        0x0000000000000000000000000000000000000000,
        0x000000000000000000000000000000000000dEaD
      )
      AND (
        c.contract_address IS NULL
        OR c.contract_project = 'Gnosis Safe'
      )
      AND address not in (
        select distinct
          address
        from
          dex_cex_addresses
      )
      AND balance > 0
    GROUP BY
      b.day,
      ts.total_supply
  )
SELECT
  htv_token."Date" AS "Date",
  COALESCE(htv_token."0-0.0000025%", 0) AS "0-0.0000025%",
  COALESCE(htv_token."0.0000025-0.000025%", 0) AS "0.0000025-0.000025%",
  COALESCE(htv_token."0.000025-0.0005%", 0) AS "0.000025-0.0005%",
  COALESCE(htv_token."0.0005-0.005%", 0) AS "0.0005-0.005%",
  COALESCE(htv_token.">.005%", 0) AS ">.005%"
FROM
  token_holders_with_token_value htv_token
ORDER BY
  htv_token."Date";