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
      SUM(tokens / POWER(10, d.decimals)) AS total_supply
    FROM
      (
        SELECT
          wallet,
          SUM(amount) AS tokens
        FROM
          (
            SELECT
              "to" AS wallet,
              contract_address,
              SUM(CAST(value AS DOUBLE)) AS amount
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
              - SUM(CAST(value AS DOUBLE)) AS amount
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
  wallet_balances AS (
    SELECT
      wallet,
      tokens / POWER(10, d.decimals) AS balance
    FROM
      (
        SELECT
          wallet,
          SUM(amount) AS tokens
        FROM
          (
            SELECT
              "to" AS wallet,
              contract_address,
              SUM(CAST(value AS DOUBLE)) AS amount
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
              - SUM(CAST(value AS DOUBLE)) AS amount
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
  )
SELECT
  range,
  COALESCE(SUM(balance), 0) AS balance
FROM
  (
    SELECT
      CASE
        WHEN wb.balance > 0
        AND wb.balance <= ts.total_supply * 0.000001 THEN '0-0.0001%'
        WHEN wb.balance > ts.total_supply * 0.000001
        AND wb.balance <= ts.total_supply * 0.001 THEN '0.0001-0.1%'
        WHEN wb.balance > ts.total_supply * 0.001
        AND wb.balance <= ts.total_supply * 0.005 THEN '0.1-0.5%'
        WHEN wb.balance > ts.total_supply * 0.005
        AND wb.balance <= ts.total_supply * 0.01 THEN '0.5-1%'
        WHEN wb.balance > ts.total_supply * 0.01 THEN '>1%'
      END AS range,
      wb.balance
    FROM
      wallet_balances wb
      CROSS JOIN token_total_supply ts
  ) grouped_balances
GROUP BY
  range
ORDER BY
  range;