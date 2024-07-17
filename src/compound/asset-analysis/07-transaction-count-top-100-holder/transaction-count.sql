WITH
  price AS (
    SELECT
      symbol,
      decimals,
      price
    FROM
      prices.usd_latest
    WHERE
      contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
    ORDER BY
      minute DESC
    LIMIT
      1
  ),
  raw AS (
    SELECT
      "from" as address,
      SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
    UNION ALL
    SELECT
      "to" as address,
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
      SUM(amount * price / POWER(10, decimals)) AS holding_usd
    FROM
      price,
      raw
    GROUP BY
      address
  ),
  top_100_holders AS (
    SELECT
      d.address,
      d.holding,
      d.holding_usd
    FROM
      distribution d
      LEFT JOIN contracts.contract_mapping c ON address = c.contract_address
    where
      address not in (
        select distinct
          address
        from
          labels.cex_{{chain}}
        union all
        select distinct
          project_contract_address
        from
          dex.trades
      )
      and address not in (
        0x0000000000000000000000000000000000000000,
        0x000000000000000000000000000000000000dEaD
      )
      AND (
        c.contract_address IS NULL
        OR c.contract_project = 'Gnosis Safe'
      )
    ORDER BY
      d.holding DESC
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
          "from" as address
        FROM
          erc20_{{chain}}.evt_Transfer
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          "to" as address
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
  COALESCE(tc.transaction_count, 0) AS transaction_count
FROM
  top_100_holders t
  LEFT JOIN transaction_counts tc ON t.address = tc.address
ORDER BY
  holding_usd DESC;