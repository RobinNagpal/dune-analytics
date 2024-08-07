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
      LEFT JOIN contracts.contract_mapping c ON wallet = c.contract_address
      CROSS JOIN token_total_supply ts
    WHERE
      wallet NOT IN (
        0x0000000000000000000000000000000000000000,
        0x000000000000000000000000000000000000dEaD
      )
      AND (
        c.contract_address IS NULL
        OR c.contract_project = 'Gnosis Safe'
      )
      AND wallet not in (
        select distinct
          address
        from
          dex_cex_addresses
      )
  ) grouped_balances
GROUP BY
  range
ORDER BY
  range;