WITH
  dex_pool_addresses AS (
    SELECT
      CAST(pool as Varchar) AS address,
      project,
      version,
      token0,
      token1
    FROM
      dex.pools
    WHERE
      blockchain = '{{chain}}'
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  price AS (
    SELECT
      erc.symbol,
      erc.decimals,
      erc.contract_address,
      AVG(dex.token_price_usd) as price
    FROM
      dex.prices_latest dex
      JOIN tokens.erc20 erc ON dex.token_address = erc.contract_address
    WHERE
      erc.contract_address = {{token_address}}
      AND dex.token_address = {{token_address}}
      AND erc.blockchain = '{{chain}}'
    GROUP BY
      erc.symbol,
      erc.decimals,
      erc.contract_address
  ),
  token_raw AS (
    SELECT
      CAST("from" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
    UNION ALL
    SELECT
      CAST("to" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
  ),
  token_distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding,
      SUM(amount * price / POWER(10, decimals)) AS holding_usd
    FROM
      price,
      token_raw
    WHERE
      price.contract_address = {{token_address}}
    GROUP BY
      address
  ),
  pool_token_holding AS (
    SELECT
      td.address,
      da.project,
      da.version,
      da.token0,
      da.token1,
      td.holding AS token_holding,
      td.holding_usd AS token_holding_usd
    FROM
      token_distribution td
      JOIN dex_pool_addresses da ON td.address = da.address
    WHERE
      td.holding > 0
  ),
  token_symbols AS (
    SELECT
      dth.address,
      dth.project,
      dth.version,
      dth.token_holding,
      dth.token_holding_usd,
      dth.token0,
      dth.token1,
      t0.symbol AS token0_symbol,
      t1.symbol AS token1_symbol
    FROM
      pool_token_holding dth
      JOIN tokens.erc20 t0 ON dth.token0 = t0.contract_address
      JOIN tokens.erc20 t1 ON dth.token1 = t1.contract_address
  )
SELECT DISTINCT
  project,
  version,
  address AS pool_address,
  CONCAT(token0_symbol, '/', token1_symbol) AS pool_symbol,
  token0,
  token1,
  token_holding,
  token_holding_usd
FROM
  token_symbols
ORDER BY
  token_holding DESC;