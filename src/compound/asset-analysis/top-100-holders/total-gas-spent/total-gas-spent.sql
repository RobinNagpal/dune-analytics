WITH
  token_details AS (
    SELECT
      symbol,
      decimals
    FROM
      tokens.erc20
    WHERE
      contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
    GROUP BY
      1,
      2
  ),
  raw AS (
    SELECT
      CAST("from" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE)) * -1 AS amount
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
  total_supply AS (
    SELECT
      SUM(amount / POWER(10, decimals)) AS total_supply
    FROM
      raw,
      token_details
  ),
  distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding
    FROM
      token_details,
      raw
    GROUP BY
      address
  ),
  top_100_holders AS (
    SELECT
      d.address,
      d.holding
    FROM
      distribution d
      CROSS JOIN total_supply ts
    ORDER BY
      d.holding DESC
    LIMIT
      100
  ),
  gas_fees AS (
    SELECT
      address,
      SUM(total_gas_used) AS total_gas_used
    FROM
      (
        SELECT
          CAST(evt."from" AS VARCHAR) AS address,
          SUM(CAST(txs.gas_used AS DOUBLE)) AS total_gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
        GROUP BY
          CAST(evt."from" AS VARCHAR)
        UNION ALL
        SELECT
          CAST(evt."to" AS VARCHAR) AS address,
          SUM(CAST(txs.gas_used AS DOUBLE)) AS total_gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
        GROUP BY
          CAST(evt."to" AS VARCHAR)
      ) AS combined
    GROUP BY
      address
  )
SELECT
  t.address,
  t.holding,
  COALESCE(gf.total_gas_used, 0) AS total_gas_used
FROM
  top_100_holders t
  LEFT JOIN gas_fees gf ON t.address = gf.address
ORDER BY
  t.holding DESC;