WITH
  price AS (
    SELECT
      symbol AS dex_symbol,
      decimals AS dex_decimals,
      price AS dex_price
    FROM
      prices.usd_latest
    WHERE
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
    ORDER BY
      minute DESC
    LIMIT
      1
  ),
  total_supply AS (
    SELECT
      sum(tokens) as net_supply
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
    WHERE
      tokens > 0
  ),
  aggregated_data AS (
    SELECT
      d.dex_symbol AS symbol,
      d.dex_decimals AS decimals,
      d.dex_price AS price,
      t.net_supply
    FROM
      price d,
      total_supply t
  )
SELECT
  symbol,
  price,
  net_supply / POWER(10, decimals) AS total_supply,
  (net_supply / POWER(10, decimals)) * price AS market_cap
FROM
  aggregated_data;