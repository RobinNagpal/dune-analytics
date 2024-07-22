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
      "from"
    UNION ALL
    SELECT
      "to" as address,
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
          case
            when address in (
              select distinct
                address
              from
                labels.cex_ethereum
            ) then 'CEX'
            when address in (
              select distinct
                project_contract_address
              from
                dex.trades
            ) then 'DEX'
            when address in (
              select distinct
                address
              from
                {{chain}}.creation_traces
            )
            and address not in (
              select distinct
                project_contract_address
              from
                dex.trades
            )
            and address not in (
              select distinct
                address
              from
                fund_address
            ) then 'Other Smart Contracts'
            when address in (
              select distinct
                address
              from
                fund_address
            ) then 'VCs/Fund'
            else 'Individual Address'
          end as type,
          SUM(amount / POWER(10, decimals)) AS amount,
          SUM(amount * price / POWER(10, decimals)) AS value,
          SUM(amount) / (
            SELECT
              SUM(amount)
            FROM
              raw
            WHERE
              address NOT IN (
                0x0000000000000000000000000000000000000000,
                0x000000000000000000000000000000000000dEaD
              )
          ) AS percent_holdings
        FROM
          price,
          raw
          LEFT JOIN contracts.contract_mapping c ON address = c.contract_address
        WHERE
          address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dEaD
          )
          AND (
            c.contract_address IS NULL
            OR c.contract_project = 'Gnosis Safe'
          )
        GROUP BY
          address
        ORDER BY
          value DESC
      ) a
      LEFT JOIN labels b ON CAST(a.address AS VARBINARY) = b.address
    WHERE
      a.value > 1
      AND type not in ('CEX', 'DEX')
    GROUP BY
      a.address,
      a.amount,
      a.value,
      a.percent_holdings
    ORDER BY
      a.percent_holdings DESC
    limit
      100
  )
SELECT
  SUM(percent_holdings_counter) AS total_percent_holdings
FROM
  top_100;