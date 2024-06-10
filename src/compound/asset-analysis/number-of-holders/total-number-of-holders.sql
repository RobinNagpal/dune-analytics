WITH holdings AS (
    SELECT DISTINCT
        address,
        holding,
        {{token_address}} AS token_address
    FROM (
        SELECT
            SUM(total) AS holding,
            address
        FROM (
            SELECT
                SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS total,
                "to" AS address
            FROM
                erc20_{{chain}}.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address = {{token_address}}
            GROUP BY
                "to"
            UNION ALL
            SELECT
                -SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS total,
                "from" AS address
            FROM
                erc20_{{chain}}.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address = {{token_address}}
            GROUP BY
                "from"
        ) t
        GROUP BY
            address
    ) t
    WHERE
        holding >= 1
)
SELECT
    COUNT(DISTINCT address) AS total_number_of_holders
FROM
    holdings;
