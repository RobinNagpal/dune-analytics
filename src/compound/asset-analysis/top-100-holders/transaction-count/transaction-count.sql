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
  transaction_counts AS (
    SELECT
      address,
      COUNT(*) AS transaction_count
    FROM
      (
        SELECT
          CAST("from" AS VARCHAR) AS address
        FROM
          erc20_{{chain}}.evt_Transfer
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          CAST("to" AS VARCHAR) AS address
        FROM
          erc20_{{chain}}.evt_Transfer
        WHERE
          contract_address = {{token_address}}
      ) tx
    GROUP BY
      address
  )
SELECT
  t.address,
  t.holding,
  t.holding_usd,
  t.percent_holdings,
  COALESCE(tc.transaction_count, 0) AS transaction_count
FROM
  top_100_holders t
  LEFT JOIN transaction_counts tc ON t.address = tc.address
ORDER BY
  transaction_count DESC;