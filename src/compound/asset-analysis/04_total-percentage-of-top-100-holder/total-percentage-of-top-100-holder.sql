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
      symbol,
      decimals
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
      "from"
    UNION ALL
    SELECT
      CAST("to" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      "to"
  ),
  fund_address AS (
    SELECT
      address,
      name
    FROM
      (
        VALUES
          (
            0x820fb25352bb0c5e03e07afc1d86252ffd2f0a18,
            'Paradigm'
          ),
          (
            0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0,
            'Jump Trading'
          )
      ) AS t (address, name)
    UNION ALL
    SELECT DISTINCT
      address,
      name
    FROM
      labels.funds
  ),
  labels AS (
    SELECT DISTINCT
      address,
      name
    FROM
      labels.all
    WHERE
      blockchain = '{{chain}}'
      AND category IN (
        'contract',
        'social',
        'institution',
        'hackers',
        'infrastructure',
        'dao',
        'ofac_sanction',
        'bridge',
        'project wallet',
        'Rollup'
      )
      AND (
        label_type = 'identifier'
        OR (
          label_type = 'persona'
          AND model_name = 'dao_framework'
        )
      )
    UNION ALL
    SELECT
      *
    FROM
      fund_address
    UNION ALL
    SELECT
      address,
      exchange AS name
    FROM
      query_2296923
    UNION ALL
    SELECT
      address,
      namespace AS name
    FROM
      {{chain}}.contracts
  ),
  top_100 as (
    SELECT
      a.percent_holdings * 100 AS percent_holdings_counter
    FROM
      (
        SELECT
          address,
          CASE
            WHEN CAST(address as VARBINARY) IN (
              SELECT DISTINCT
                address
              FROM
                labels.cex_ethereum
            )
            OR CAST(address as VARBINARY) IN (
              SELECT DISTINCT
                address
              FROM
                query_2296923
            ) THEN 'CEX'
            WHEN CAST(address as VARBINARY) IN (
              SELECT DISTINCT
                project_contract_address
              FROM
                dex.trades
            ) THEN 'DEX'
            WHEN CAST(address as VARBINARY) IN (
              SELECT DISTINCT
                address
              FROM
                {{chain}}.creation_traces
            )
            AND CAST(address as VARBINARY) NOT IN (
              SELECT DISTINCT
                project_contract_address
              FROM
                dex.trades
            )
            AND CAST(address as VARBINARY) NOT IN (
              SELECT DISTINCT
                address
              FROM
                fund_address
            ) THEN 'Other Smart Contracts'
            WHEN CAST(address as VARBINARY) IN (
              SELECT DISTINCT
                address
              FROM
                fund_address
            ) THEN 'VCs/Fund'
            ELSE 'Individual Address'
          END AS address_type,
          SUM(amount / POWER(10, decimals)) AS amount,
          SUM(amount * price / POWER(10, decimals)) AS value,
          SUM(amount) / (
            SELECT
              SUM(amount)
            FROM
              raw
            WHERE
              address NOT IN (
                '0x0000000000000000000000000000000000000000',
                '0x000000000000000000000000000000000000dEaD'
              )
          ) AS percent_holdings
        FROM
          price,
          raw
        WHERE
          address NOT IN (
            '0x0000000000000000000000000000000000000000',
            '0x000000000000000000000000000000000000dEaD'
          )
        GROUP BY
          address
        ORDER BY
          value DESC
        LIMIT
          100
      ) a
      LEFT JOIN labels b ON CAST(a.address AS VARBINARY) = b.address
    WHERE
      a.value > 1
    GROUP BY
      a.address,
      a.address_type,
      a.amount,
      a.value,
      a.percent_holdings
    ORDER BY
      a.percent_holdings DESC
  )
SELECT
  SUM(percent_holdings_counter) AS total_percent_holdings
FROM
  top_100;