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
)
SELECT
    CASE
        WHEN percent_holdings >= 0.5 THEN 'H) Holdings >=50%'
        WHEN percent_holdings >= 0.4 AND percent_holdings < 0.5 THEN 'G) Holdings >=40% & <50%'
        WHEN percent_holdings >= 0.3 AND percent_holdings < 0.4 THEN 'F) Holdings >=30% & <40%'
        WHEN percent_holdings >= 0.2 AND percent_holdings < 0.3 THEN 'E) Holdings >=20% & <30%'
        WHEN percent_holdings >= 0.1 AND percent_holdings < 0.2 THEN 'D) Holdings >=10% & <20%'
        WHEN percent_holdings >= 0.05 AND percent_holdings < 0.1 THEN 'C) Holdings >=5% & <10%'
        WHEN percent_holdings >= 0.01 AND percent_holdings < 0.05 THEN 'B) Holdings >=1% & <5%'
        WHEN percent_holdings < 0.01 THEN 'A) Holdings <1%'
    END AS distribution,
    COUNT(DISTINCT address) AS address_count,
    SUM(holding) AS total_holding
FROM
    (
        SELECT
            address,
            SUM(amount / POWER(10, decimals)) AS holding,
            SUM(amount * price / POWER(10, decimals)) AS holding_usd,
            SUM(amount) / (
                SELECT
                    SUM(amount)
                FROM
                    raw
                WHERE
                    address NOT IN (
                        0x0000000000000000000000000000000000000000,
                        0x000000000000000000000000000000000000dEaD,
                        0xD15a672319Cf0352560eE76d9e89eAB0889046D3
                    )
            ) AS percent_holdings
        FROM
            price,
            raw
        WHERE
            address NOT IN (
                0x0000000000000000000000000000000000000000,
                0x000000000000000000000000000000000000dEaD,
                0xD15a672319Cf0352560eE76d9e89eAB0889046D3
            )
        GROUP BY
            address
    ) a
WHERE
    holding_usd > 1
GROUP BY
    distribution;
