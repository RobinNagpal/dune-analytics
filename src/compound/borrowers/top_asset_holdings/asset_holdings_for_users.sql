WITH holdings AS (
    SELECT
        address,
        SUM(total) AS holding,
        contract_address,
        MAX(symbol) AS symbol
    FROM
        (
            SELECT
                to AS address,
                SUM(cast(value as double) / pow(10, b.decimals)) AS total,
                a.contract_address,
                b.symbol
            FROM
                erc20_ethereum.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address IN (
                    0xc3d688b66703497daa19211eedff47f25384cdc3,
                    0xdAC17F958D2ee523a2206206994597C13D831ec7,
                    0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE
                )
            GROUP BY
                to,
                a.contract_address,
                b.symbol
            UNION
            ALL
            SELECT
                "from" AS address,
                - SUM(cast(value as double) / pow(10, b.decimals)) AS total,
                a.contract_address,
                b.symbol
            FROM
                erc20_ethereum.evt_Transfer a
                JOIN tokens.erc20 b ON a.contract_address = b.contract_address
            WHERE
                a.contract_address IN (
                    0xc3d688b66703497daa19211eedff47f25384cdc3,
                    0xdAC17F958D2ee523a2206206994597C13D831ec7,
                    0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE
                )
            GROUP BY
                "from",
                a.contract_address,
                b.symbol
        ) t
    WHERE
        address IN (
            0x2ed5eaf929fee1f5f9b32d83db8ed06b52692a74,
            0xa118d7d5460ba8f52d7633ef3a94091d9a12fae7,
            0xf3B0073E3a7F747C7A38B36B805247B222C302A3
        )
    GROUP BY
        address,
        contract_address
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY contract_address
        ORDER BY
            holding DESC
    ) AS Ranking,
    address AS Wallet_Address,
    holding AS Amount_Held,
    contract_address,
    symbol
FROM
    holdings
ORDER BY
    contract_address,
    Ranking;