WITH
  dex_addresses AS (
    SELECT
      CAST(address as Varchar) AS address,
      dex_name
    FROM
      dex.addresses
    WHERE
      blockchain = '{{chain}}'
    GROUP BY
      1,
      2
    UNION ALL
    SELECT
      CAST(project_contract_address as Varchar) AS address,
      project as name
    FROM
      dex.trades
    WHERE
      blockchain = '{{chain}}'
    GROUP BY
      1,
      2
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
  dex_token_holding AS (
    SELECT
      td.address,
      da.dex_name,
      td.holding AS token_holding,
      td.holding_usd AS token_holding_usd
    FROM
      token_distribution td
      JOIN dex_addresses da ON td.address = da.address
    WHERE
      td.holding > 0.0000001
  )
SELECT
  *
FROM
  dex_token_holding
ORDER BY
  token_holding DESC;