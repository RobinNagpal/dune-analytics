WITH price AS (
    SELECT
        symbol,
        decimals,
        AVG(token_price_usd) AS price
    FROM
        dex.prices_latest
    JOIN tokens.erc20 ON contract_address = {{token_address}}
    WHERE
        token_address = {{token_address}}
        AND blockchain = '{{chain}}'
    GROUP BY
        symbol,
        decimals
),
raw AS (
    SELECT
        "from" AS address,
        SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
        erc20_{{chain}}.evt_Transfer
    WHERE
        contract_address = {{token_address}}
    GROUP BY
        "from"
    UNION ALL
    SELECT
        "to" AS address,
        SUM(CAST(value AS DOUBLE)) AS amount
    FROM
        erc20_{{chain}}.evt_Transfer
    WHERE
        contract_address = {{token_address}}
    GROUP BY
        "to"
),
holdings AS (
    SELECT
        address,
        SUM(amount / POWER(10, decimals)) AS holding,
        SUM(amount * price / POWER(10, decimals)) AS holding_usd
    FROM
        price,
        raw
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dEaD
        )
    GROUP BY
        address,
        decimals,
        price
),
dex_cex_addresses AS (
    SELECT
      address AS address
    FROM
      cex.addresses
    WHERE
      blockchain = '{{chain}}'
    UNION ALL
    SELECT
      address
    FROM
      (
        SELECT
          address AS address
        FROM
          dex.addresses
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
        UNION ALL
        SELECT
          project_contract_address AS address
        FROM
          dex.trades
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
      )
  ),
categorized_holdings AS (
    SELECT
        address,
        holding,
        holding_usd,
        CASE
            WHEN holding_usd BETWEEN 0 AND 100 THEN '0-$100'
            WHEN holding_usd > 100 AND holding_usd <= 1000 THEN '$100-$1K'
            WHEN holding_usd > 1000 AND holding_usd <= 5000 THEN '$1K-$5K'
            WHEN holding_usd > 5000 AND holding_usd <= 10000 THEN '$5K-$10K'
            WHEN holding_usd > 10000 AND holding_usd <= 50000 THEN '$10K-$50K'
            WHEN holding_usd > 50000 AND holding_usd <= 250000 THEN '$50K-$250K'
            WHEN holding_usd > 250000 THEN '$250K+'
        END AS holding_category
    FROM
        holdings
        LEFT JOIN contracts.contract_mapping c ON address = c.contract_address
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dEaD
          )
          AND (
            c.contract_address IS NULL
            OR c.contract_project = 'Gnosis Safe'
          ) AND address NOT IN (select distinct
          address
        from
          dex_cex_addresses) AND
        holding_usd > 0
)
SELECT
    holding_category,
    COUNT(DISTINCT address) AS address_count
FROM
    categorized_holdings
GROUP BY
    holding_category
ORDER BY
    (CASE
        WHEN holding_category = '0-$100' THEN 1
        WHEN holding_category = '$100-$1K' THEN 2
        WHEN holding_category = '$1K-$5K' THEN 3
        WHEN holding_category = '$5K-$10K' THEN 4
        WHEN holding_category = '$10K-$50K' THEN 5
        WHEN holding_category = '$50K-$250K' THEN 6
        WHEN holding_category = '$250K+' THEN 7
    END);
