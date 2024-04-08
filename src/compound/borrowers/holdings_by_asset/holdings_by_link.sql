WITH holdings AS (
    SELECT
        address,
        SUM(total) AS holding
    FROM
        (
            SELECT
                "to" AS address,
                SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS total
            FROM
                erc20_ethereum.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
            GROUP BY
                "to"
            UNION
            ALL
            SELECT
                "from" AS address,
                - SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS total
            FROM
                erc20_ethereum.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
            GROUP BY
                "from"
        ) t
    GROUP BY
        address
),
compiled AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                a.holding DESC
        ) AS Ranking,
        a.holding AS Amount_Held,
        a.holding * (
            SELECT
                price
            FROM
                prices.usd_latest
            WHERE
                blockchain = 'ethereum'
                AND contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
        ) AS Value_of_Holdings,
        CASE
            WHEN 'ethereum' = 'ethereum' THEN '<a href=https://etherscan.io//address/' || CAST(a.address AS VARCHAR) || ' target=_blank">' || CAST(a.address AS VARCHAR) || '</a>'
        END AS wallet_address
    FROM
        holdings a
),
balance_classification AS (
    SELECT
        wallet_address,
        Value_of_Holdings,
        CASE
            WHEN Value_of_Holdings < 0.01 THEN '1. [0, 0.01) USD'
            WHEN Value_of_Holdings >= 0.01
            AND Value_of_Holdings < 1 THEN '2. [0.01, 1) USD'
            WHEN Value_of_Holdings >= 1
            AND Value_of_Holdings < 10 THEN '3. [1,10) USD'
            WHEN Value_of_Holdings >= 10
            AND Value_of_Holdings < 100 THEN '4. [10,100) USD'
            WHEN Value_of_Holdings >= 100
            AND Value_of_Holdings < 200 THEN '5. [100,200) USD'
            WHEN Value_of_Holdings >= 200
            AND Value_of_Holdings < 500 THEN '6. [200,500) USD'
            WHEN Value_of_Holdings >= 500
            AND Value_of_Holdings < 1000 THEN '7. [500,1000) USD'
            WHEN Value_of_Holdings >= 1000
            AND Value_of_Holdings < 2000 THEN '8. [1000,2000) USD'
            WHEN Value_of_Holdings >= 2000
            AND Value_of_Holdings < 5000 THEN '9. [2000,5000) USD'
            WHEN Value_of_Holdings >= 5000
            AND Value_of_Holdings < 10000 THEN '91. [5000,10000) USD'
            WHEN Value_of_Holdings >= 10000
            AND Value_of_Holdings < 20000 THEN '92. [1W,2W) USD'
            WHEN Value_of_Holdings >= 20000
            AND Value_of_Holdings < 100000 THEN '93. [ 2W,10W) USD'
            WHEN Value_of_Holdings >= 100000
            AND Value_of_Holdings < 1000000 THEN '94. [ 10W,100W) USD'
            WHEN Value_of_Holdings >= 1000000
            AND Value_of_Holdings < 10000000 THEN '95. [ 100W,1000W) USD'
            WHEN Value_of_Holdings >= 10000000 THEN '96.  [1000W, ...) USD'
        END AS erc20_usd_Holdings,
        CASE
            WHEN Amount_Held < 100 THEN '1. [0, 100) LINK'
            WHEN Amount_Held >= 100
            AND Amount_Held < 500 THEN '2. [100, 500) LINK'
            WHEN Amount_Held >= 500
            AND Amount_Held < 1000 THEN '3. [500, 1000) LINK'
            WHEN Amount_Held >= 1000 THEN '4. [1000, ...) LINK'
        END AS chainlink_holdings
    FROM
        compiled a
    ORDER BY
        2 DESC
)
SELECT
    erc20_usd_Holdings,
    chainlink_holdings,
    COUNT(wallet_address) AS Addresses
FROM
    balance_classification
GROUP BY
    erc20_usd_Holdings,
    chainlink_holdings
ORDER BY
    COUNT(wallet_address) DESC;