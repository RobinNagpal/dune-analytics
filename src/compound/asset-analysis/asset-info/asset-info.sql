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
      token_address = {{token_address}} AND
      contract_address = {{token_address}} AND
      blockchain = '{{chain}}'
    GROUP BY
      dex_symbol,
      dex_decimals
  ),
  supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}} AND
      ("from" = 0x0000000000000000000000000000000000000000 OR
      "to" IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3))
  )
SELECT
  dex_symbol AS symbol,
  dex_price AS price,
  SUM(amount * dex_price / POWER(10, dex_decimals)) AS market_cap,
  SUM(amount / POWER(10, dex_decimals)) AS circulating_supply
FROM
  supply,
  dex_price
GROUP BY
  dex_symbol,
  dex_price;
