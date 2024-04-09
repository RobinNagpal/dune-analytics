WITH link_price AS (
    SELECT
        price,
        (SELECT symbol FROM tokens.erc20 WHERE contract_address = {{token_address}}  LIMIT 1) AS symbol
    FROM
        prices.usd_latest
    WHERE
        blockchain = 'ethereum'
        AND contract_address = {{token_address}}
    LIMIT
        1
), withdrawers AS (
    SELECT
        to AS address
    FROM
        compound_v3_ethereum.Comet_evt_Withdraw
    GROUP BY
        to
),
holdings AS (
    SELECT
        w.address,
        SUM(total) AS holding
    FROM
        withdrawers w
        JOIN (
            SELECT
                "to" AS address,
                SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS total
            FROM
                erc20_ethereum.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address = {{token_address}}
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
                a.contract_address = {{token_address}}
            GROUP BY
                "from"
        ) transfers ON w.address = transfers.address
    GROUP BY
        w.address
),
compiled AS (
    SELECT
        ROUND(
            a.holding * (
                SELECT
                    price
                from
                    link_price
            ),
            2
        ) AS Value_of_Holdings,
        a.address AS wallet_address
    FROM
        holdings a
),
balance_classification AS (
    SELECT
        wallet_address,
        Value_of_Holdings,
        CASE
            WHEN Value_of_Holdings < 50 THEN '[0, 50) USD'
            WHEN Value_of_Holdings >= 50
            AND Value_of_Holdings < 100 THEN '[50, 100) USD'
            WHEN Value_of_Holdings >= 100
            AND Value_of_Holdings < 200 THEN '[100,200) USD'
            WHEN Value_of_Holdings >= 200
            AND Value_of_Holdings < 500 THEN '[200,500) USD'
            WHEN Value_of_Holdings >= 500
            AND Value_of_Holdings < 1000 THEN '[500,1000) USD'
            WHEN Value_of_Holdings >= 1000
            AND Value_of_Holdings < 2000 THEN '[1000,2000) USD'
            WHEN Value_of_Holdings >= 2000
            AND Value_of_Holdings < 5000 THEN '[2000,5000) USD'
            WHEN Value_of_Holdings >= 5000
            AND Value_of_Holdings < 10000 THEN '[5000,10000) USD'
            WHEN Value_of_Holdings >= 10000
            AND Value_of_Holdings < 20000 THEN '[1W,2W) USD'
            WHEN Value_of_Holdings >= 20000
            AND Value_of_Holdings < 100000 THEN '[ 2W,10W) USD'
            WHEN Value_of_Holdings >= 100000
            AND Value_of_Holdings < 1000000 THEN '[ 10W,100W) USD'
            WHEN Value_of_Holdings >= 1000000
            AND Value_of_Holdings < 10000000 THEN '[ 100W,1000W) USD'
            WHEN Value_of_Holdings >= 10000000 THEN '[1000W, ...) USD'
        END AS erc20_usd_Holdings,
        CASE
            WHEN Value_of_Holdings < 50 THEN CONCAT(
                '[0, ',
                CAST(
                    CAST(
                        ROUND(
                            50 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 50
            AND Value_of_Holdings < 100 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            50 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            100 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ', 
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 100
            AND Value_of_Holdings < 200 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            100 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            200 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ', 
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 200
            AND Value_of_Holdings < 500 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            200 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            500 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 500
            AND Value_of_Holdings < 1000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            500 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            1000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 1000
            AND Value_of_Holdings < 2000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            1000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            2000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 2000
            AND Value_of_Holdings < 5000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            2000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            5000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 5000
            AND Value_of_Holdings < 10000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            5000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            10000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 10000
            AND Value_of_Holdings < 20000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            10000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            20000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)

            )
            WHEN Value_of_Holdings >= 20000
            AND Value_of_Holdings < 100000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            20000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            100000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 100000
            AND Value_of_Holdings < 1000000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            100000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            1000000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 1000000
            AND Value_of_Holdings < 10000000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            1000000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                CAST(
                    CAST(
                        ROUND(
                            10000000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ') ',
                (SELECT symbol FROM link_price)
            )
            WHEN Value_of_Holdings >= 10000000 THEN CONCAT(
                '[',
                CAST(
                    CAST(
                        ROUND(
                            10000000 / (
                                SELECT
                                    price
                                FROM
                                    link_price
                            ),
                            2
                        ) AS DECIMAL(18, 2)
                    ) AS VARCHAR
                ),
                ', ',
                '...) ',
                (SELECT symbol FROM link_price)
            )
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
    MIN(Value_of_Holdings);