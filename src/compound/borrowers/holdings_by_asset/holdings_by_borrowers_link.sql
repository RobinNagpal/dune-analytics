WITH holdings AS (
    SELECT
        src AS address,
        SUM(amount / POW(10, b.decimals)) AS total_withdrawal
    FROM
        compound_v3_ethereum.Comet_evt_Withdraw a
        JOIN tokens.erc20 b ON a.contract_address = b.contract_address
    WHERE
        a.contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
    GROUP BY
        src
),
compiled AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                a.total_withdrawal DESC
        ) AS Ranking,
        a.total_withdrawal AS Amount_Withdrawn,
        a.total_withdrawal * (
            SELECT
                price
            FROM
                prices.usd_latest
            WHERE
                blockchain = 'ethereum'
                AND contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
        ) AS Value_of_Withdrawals,
        CASE
            WHEN 'ethereum' = 'ethereum' THEN '<a href=https://etherscan.io//address/' || CAST(a.address AS VARCHAR) || ' target=_blank">' || CAST(a.address AS VARCHAR) || '</a>'
        END AS wallet_address
    FROM
        holdings a
),
balance_classification AS (
    SELECT
        wallet_address,
        Value_of_Withdrawals,
        CASE
            WHEN Value_of_Withdrawals < 0.01 THEN '1. [0, 0.01) USD'
            WHEN Value_of_Withdrawals >= 0.01
            AND Value_of_Withdrawals < 1 THEN '2. [0.01, 1) USD'
            WHEN Value_of_Withdrawals >= 1
            AND Value_of_Withdrawals < 10 THEN '3. [1,10) USD'
            WHEN Value_of_Withdrawals >= 10
            AND Value_of_Withdrawals < 100 THEN '4. [10,100) USD'
            WHEN Value_of_Withdrawals >= 100
            AND Value_of_Withdrawals < 200 THEN '5. [100,200) USD'
            WHEN Value_of_Withdrawals >= 200
            AND Value_of_Withdrawals < 500 THEN '6. [200,500) USD'
            WHEN Value_of_Withdrawals >= 500
            AND Value_of_Withdrawals < 1000 THEN '7. [500,1000) USD'
            WHEN Value_of_Withdrawals >= 1000
            AND Value_of_Withdrawals < 2000 THEN '8. [1000,2000) USD'
            WHEN Value_of_Withdrawals >= 2000
            AND Value_of_Withdrawals < 5000 THEN '9. [2000,5000) USD'
            WHEN Value_of_Withdrawals >= 5000
            AND Value_of_Withdrawals < 10000 THEN '91. [5000,10000) USD'
            WHEN Value_of_Withdrawals >= 10000
            AND Value_of_Withdrawals < 20000 THEN '92. [1W,2W) USD'
            WHEN Value_of_Withdrawals >= 20000
            AND Value_of_Withdrawals < 100000 THEN '93. [ 2W,10W) USD'
            WHEN Value_of_Withdrawals >= 100000
            AND Value_of_Withdrawals < 1000000 THEN '94. [ 10W,100W) USD'
            WHEN Value_of_Withdrawals >= 1000000
            AND Value_of_Withdrawals < 10000000 THEN '95. [ 100W,1000W) USD'
            WHEN Value_of_Withdrawals >= 10000000 THEN '96.  [1000W, ...) USD'
        END AS erc20_usd_Holdings,
        CASE
            WHEN Amount_Withdrawn < 100 THEN '1. [0, 100) LINK'
            WHEN Amount_Withdrawn >= 100
            AND Amount_Withdrawn < 500 THEN '2. [100, 500) LINK'
            WHEN Amount_Withdrawn >= 500
            AND Amount_Withdrawn < 1000 THEN '3. [500, 1000) LINK'
            WHEN Amount_Withdrawn >= 1000 THEN '4. [1000, ...) LINK'
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