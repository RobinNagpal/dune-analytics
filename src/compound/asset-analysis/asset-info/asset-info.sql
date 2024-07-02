WITH
  dex_price AS (
    SELECT
      symbol AS dex_symbol,
      decimals AS dex_decimals,
      AVG(token_price_usd) AS dex_price
    FROM
      dex.prices_latest,
      tokens.erc20
    WHERE
      token_address = {{token_address}}
      AND contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
    GROUP BY
      symbol,
      decimals
  ),
  minted_supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS minted_amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "from" = 0x0000000000000000000000000000000000000000
  ),
  burned_supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS burned_amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "to" IN (0x0000000000000000000000000000000000000000)
  ),
  total_supply AS (
    SELECT
      (
        COALESCE(m.minted_amount, 0) - COALESCE(b.burned_amount, 0)
      ) AS net_supply
    FROM
      minted_supply m
      CROSS JOIN burned_supply b
  ),
  aggregated_data AS (
    SELECT
      d.dex_symbol AS symbol,
      d.dex_decimals AS decimals,
      d.dex_price AS price,
      t.net_supply
    FROM
      dex_price d,
      total_supply t
  )
SELECT
  symbol,
  price,
  net_supply / POWER(10, decimals) AS total_supply,
  (net_supply / POWER(10, decimals)) * price AS market_cap
FROM
  aggregated_data;