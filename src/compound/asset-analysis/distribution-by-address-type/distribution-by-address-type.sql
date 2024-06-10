WITH price AS (
    SELECT
        symbol,
        decimals,
        AVG(token_price_usd) AS price
    FROM
        dex.prices_latest,
        tokens.erc20
    WHERE
        token_address = {{Token Contract Address}}
        AND contract_address = {{Token Contract Address}}
        AND blockchain = '{{Blockchain}}'
    GROUP BY
        symbol,
        decimals
),
raw AS (
    SELECT
        "from" AS address,
        SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
        erc20_{{Blockchain}}.evt_Transfer
    WHERE
        contract_address = {{Token Contract Address}}
    GROUP BY
        "from"
    UNION ALL
    SELECT
        "to" AS address,
        SUM(CAST(value AS DOUBLE)) AS amount
    FROM
        erc20_{{Blockchain}}.evt_Transfer
    WHERE
        contract_address = {{Token Contract Address}}
    GROUP BY
        "to"
),
fund_address AS (
    SELECT
        address
    FROM
        (
            VALUES
                (0x820fb25352bb0c5e03e07afc1d86252ffd2f0a18, 'Paradigm'),
                (0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0, 'Jump Trading')
        ) AS t (address, name)
    UNION ALL
    SELECT DISTINCT
        address
    FROM
        labels.funds
)
SELECT
    type,
    SUM(amount) AS total_holdings
FROM
    (
        SELECT
            address,
            CASE
                WHEN address IN (
                    SELECT DISTINCT address FROM cex_evms.addresses
                )
                OR address IN (
                    SELECT DISTINCT address FROM query_2296923
                ) THEN 'CEX'
                WHEN address IN (
                    SELECT DISTINCT project_contract_address FROM dex.trades
                ) THEN 'DEX'
                WHEN address IN (
                    SELECT DISTINCT address FROM safe.safes_all
                ) THEN 'Multi-Sig Wallet'
                WHEN address IN (
                    SELECT DISTINCT address FROM {{Blockchain}}.creation_traces
                )
                AND address NOT IN (
                    SELECT DISTINCT project_contract_address FROM dex.trades
                )
                AND address NOT IN (
                    SELECT DISTINCT address FROM safe.safes_all
                )
                AND address NOT IN (
                    SELECT DISTINCT address FROM fund_address
                ) THEN 'Other Smart Contracts'
                WHEN address IN (
                    SELECT DISTINCT address FROM fund_address
                ) THEN 'VCs/Fund'
                ELSE 'Individual Address'
            END AS type,
            SUM(amount / POWER(10, decimals)) AS amount,
            SUM(amount * price / POWER(10, decimals)) AS value
        FROM
            price,
            raw
        WHERE
            address <> 0x0000000000000000000000000000000000000000
        GROUP BY
            address,
            type
    ) a
WHERE
    value > 1
GROUP BY
    type;
