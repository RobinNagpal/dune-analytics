# About

This graph shows how the total supply of a token is distributed among the token holders

# Graph

![distributionByHoldingPercentage](distribution-by-holding-percentage.png)

# Relevance

## Asset decentralization

    Token distribution by holding % tells us how decentralized or centeralized the asset is. A more decentralized distribution, in which large number of addresses hold smaller percentages of the total supply, suggests a good ecosystem. On the other hand, a distribution, where a few addresses hold large portions of the supply, indicates market manipulation, liquidity issues or governance issues

## Holder Categories

- Investors: addresses holding less than 1% of the total supply represent retail investors. A large number of such addresses indicate broad adoption and support.
- Whales: Addresses holding more than 5-10% of the total supply are considered whales. Monitoring whale activity can provide insights into potential market movements or strategic investments.
- Institutional Investors: Addresses holding significant but not excessively large percentages (e.g., 1-5%) might represent institutional investors or large stakeholders.

# Query Explanation

This query calculates the distribution of token holdings by categorizing addresses into various percentage ranges. It considers the price and decimals of the token, sums the incoming and outgoing transfers to determine net holdings, and calculates the holding value in USD. The query then categorizes these holdings into ranges, counts the number of addresses in each range, and sums the total holdings for each range, filtering out addresses with negligible holdings.

Price CTE calculates the average price of the specified token and retrieves its symbol and decimals

```sql
price AS (
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
)
```

Raw CTE calculates the net amount of tokens held by each address by summing up incoming and outgoing transfers

```sql
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
```

Finally categorizes addresses based on their percentage holdings, counts the number of addresses in each category, and sums their total holdings.

- Categorizes addresses based on percent_holdings into various ranges.
- Counts the number of distinct addresses (address_count) in each category.
- Sums the total holdings (total_holding) for each category.
- Filters out addresses with a holding value of less than $1 USD as well as null and burner addresses
- Groups by the holding percentage category.

```sql
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
```

## Tables used

- dex.prices_latest
- tokens.erc20
- erc20\_{{Blockchain}}.evt_Transfer

## Alternative Choices
