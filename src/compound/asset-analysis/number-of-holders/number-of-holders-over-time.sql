WITH daily_holdings AS (
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        "to" AS address,
        SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS balance
    FROM
        erc20_{{chain}}.evt_Transfer a
        JOIN tokens.erc20 b ON a.contract_address = b.contract_address
    WHERE
        a.contract_address = {{token_address}}
    GROUP BY
        day,
        address
),
daily_balances AS (
    SELECT
        day,
        address,
        SUM(balance) OVER (
            PARTITION BY address
            ORDER BY day
        ) AS cumulative_balance
    FROM
        daily_holdings
),
filtered_balances AS (
    SELECT
        day,
        address,
        cumulative_balance
    FROM
        daily_balances
    WHERE
        cumulative_balance > 0
),
distinct_holders AS (
    SELECT DISTINCT
        day,
        address
    FROM
        filtered_balances
),
first_seen AS (
    SELECT
        address,
        MIN(day) AS first_seen_day
    FROM
        distinct_holders
    GROUP BY
        address
),
holders_per_day AS (
    SELECT
        first_seen_day AS day,
        COUNT(*) AS new_holders
    FROM
        first_seen
    GROUP BY
        first_seen_day
),
cumulative_holders AS (
    SELECT
        day,
        SUM(new_holders) OVER (
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_number_of_holders
    FROM
        holders_per_day
)
SELECT
    day,
    cumulative_number_of_holders
FROM
    cumulative_holders
ORDER BY
    day;
