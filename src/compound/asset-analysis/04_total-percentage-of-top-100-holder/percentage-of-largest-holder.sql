WITH price AS (
    SELECT
        symbol,
        decimals,
        AVG(token_price_usd) AS price
    FROM
        dex.prices_latest,
        tokens.erc20
    WHERE
        token_address = {{token_address}} AND
        contract_address = {{token_address}} AND
        blockchain = '{{chain}}'
    GROUP BY
        symbol, decimals
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
fund_address AS (
    SELECT
        address,
        name
    FROM
        (VALUES
            (0x820fb25352bb0c5e03e07afc1d86252ffd2f0a18, 'Paradigm'),
            (0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0, 'Jump Trading')
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
        blockchain = '{{chain}}' AND
        category IN ('contract', 'social', 'institution', 'hackers', 'infrastructure', 'dao', 'ofac_sanction', 'bridge', 'project wallet', 'Rollup') AND
        (label_type = 'identifier' OR (label_type = 'persona' AND model_name = 'dao_framework'))
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
)
SELECT
    CASE
        WHEN '{{chain}}' = 'ethereum' THEN CONCAT('<a href="https://etherscan.io/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'arbitrum' THEN CONCAT('<a href="https://arbiscan.io/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'polygon' THEN CONCAT('<a href="https://polygonscan.com/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'avalanche_c' THEN CONCAT('<a href="https://snowtrace.io/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'bnb' THEN CONCAT('<a href="https://bscscan.com/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'optimism' THEN CONCAT('<a href="https://optimistic.etherscan.io/address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'fantom' THEN CONCAT('<a href="https://ftmscan.com//address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        WHEN '{{chain}}' = 'base' THEN CONCAT('<a href="https://basescan.org//address/', CAST(a.address AS VARCHAR), '" target="_blank">', 'Block Explorer', '</a>')
        ELSE CAST(a.address AS VARCHAR)
    END AS address,
    ARRAY_AGG(DISTINCT name) AS labels,
    type,
    amount,
    value,
    percent_holdings,
    percent_holdings * 100 AS percent_holdings_counter
FROM
    (SELECT
         address,
         CASE
             WHEN address IN (SELECT DISTINCT address FROM labels.cex_ethereum) OR address IN (SELECT DISTINCT address FROM query_2296923) THEN 'CEX'
             WHEN address IN (SELECT DISTINCT project_contract_address FROM dex.trades) THEN 'DEX'
             WHEN address IN (SELECT DISTINCT address FROM {{chain}}.creation_traces) AND address NOT IN (SELECT DISTINCT project_contract_address FROM dex.trades) AND address NOT IN (SELECT DISTINCT address FROM fund_address) THEN 'Other Smart Contracts'
             WHEN address IN (SELECT DISTINCT address FROM fund_address) THEN 'VCs/Fund'
             ELSE 'Individual Address'
         END AS type,
         SUM(amount / POWER(10, decimals)) AS amount,
         SUM(amount * price / POWER(10, decimals)) AS value,
         SUM(amount) / (SELECT SUM(amount) FROM raw WHERE address NOT IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3)) AS percent_holdings
     FROM
         price,
         raw
     WHERE
         address NOT IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3)
     GROUP BY
         address, type
     ORDER BY
         value DESC
     LIMIT
         100
    ) a
    LEFT JOIN labels b ON CAST(a.address AS VARBINARY) = b.address
WHERE
    value > 1
GROUP BY
    address, type, amount, value, percent_holdings
ORDER BY
    percent_holdings DESC;
