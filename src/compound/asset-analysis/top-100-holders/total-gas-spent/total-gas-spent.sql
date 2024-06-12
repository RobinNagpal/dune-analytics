WITH
  price AS (
    SELECT
      symbol,
      decimals,
      AVG(token_price_usd) AS price
    FROM
      dex.prices_latest,
      tokens.erc20
    WHERE
      token_address = {{token_address}}
      AND contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
    GROUP BY
      1,
      2
  ),
  raw AS (
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
  distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding,
      SUM(amount * price / POWER(10, decimals)) AS holding_usd,
      SUM(amount) / (
        SELECT
          SUM(amount)
        FROM
          raw
      ) AS percent_holdings
    FROM
      price,
      raw
    GROUP BY
      address
  ),
  top_100_holders AS (
    SELECT
      address,
      holding,
      holding_usd,
      percent_holdings
    FROM
      distribution
    ORDER BY
      holding DESC
    LIMIT
      100
  ),
  gas_fees AS (
    SELECT
      address,
      SUM(CAST(gas_used AS DOUBLE)) AS total_gas_used
    FROM
      (
        SELECT
          CAST(evt."from" AS VARCHAR) AS address,
          gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
        UNION ALL
        SELECT
          CAST(evt."to" AS VARCHAR) AS address,
          gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
      ) gas_tx
    GROUP BY
      address
  )
SELECT
  t.address,
  t.holding,
  t.holding_usd,
  t.percent_holdings,
  COALESCE(gf.total_gas_used, 0) AS total_gas_used
FROM
  top_100_holders t
  LEFT JOIN gas_fees gf ON t.address = gf.address
ORDER BY
  t.holding DESC;