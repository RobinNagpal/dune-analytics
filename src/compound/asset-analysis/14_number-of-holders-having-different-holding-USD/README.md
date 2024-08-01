# About

This query calculates and visualizes the number of token holders over time based on the USD value of their token holdings. The holders are categorized into different value ranges: $0 - $100, $100 - $1,000, $1,000 - $10,000, $10,000 - $100,000, and $100,000+.

# Graph

![numberOfHoldersHavingDifferentHoldingUSD](number-of-holders-different-holding-USD.png)

# Relevance

Understanding the distribution of wealth among token holders is crucial for analyzing market dynamics, investor behavior, and the overall health of a cryptocurrency project. By categorizing holders into different value ranges, stakeholders can gain a clearer picture of how wealth is distributed, which can influence decisions related to marketing, development, and strategic partnerships.
Analyzing changes in these values over time can reveal trends in wealth distribution.
A rise in the "100000+" category might suggest whales accumulating tokens, while a rise in "0-100" could indicate growing retail interest.

# Query Explanation

The query performs the following steps:

- Aggregates daily token transfer amounts for each address.
- Computes cumulative token balances for each address over time.
- Generates a sequence of days to ensure all days are accounted for.
- Calculates daily token balances for each address.
- Retrieves the average daily price of the token.
- Determines the number of token holders in various value ranges based on their token holdings' USD value.
- Outputs the number of holders for each value range on a daily basis.

## Number of tokens transferred in and out

Extracts daily token transfer events for the specified token, excluding self-transfers, and calculates daily net token balances for each address by summing incoming and outgoing transfer values.

```sql
transfers AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS DAY,
      contract_address,
      "from",
      to,
      value
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "from" <> to
  ),
  balances AS (
    SELECT
      DAY,
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      DAY,
      contract_address,
      to
    UNION ALL
    SELECT
      DAY,
      contract_address,
      "from" AS address,
      - SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      DAY,
      contract_address,
      "from"
  ),
```

## Token Balances with Gap Days

This Common Table Expression (CTE) calculates the cumulative token balance for each address up to each day.

```sql
token_balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(balance) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS balance,
      LEAD(DAY, 1, CURRENT_TIMESTAMP) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS next_day
    FROM
      balances AS t
  ),
```

## Days Sequence

This CTE generates a sequence of days from January 1, 2021, to the current day.

```sql
days AS (
    SELECT
      DAY
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2021-01-01' AS TIMESTAMP),
          CAST(
            TRY_CAST(
              TRY_CAST(
                TRY_CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP) AS TIMESTAMP
              ) AS TIMESTAMP
            ) AS TIMESTAMP
          ),
          INTERVAL '1' day
        )
      ) AS _u (DAY)
  )
```

## Token Balance of All Days

This CTE ensures that balances are carried forward for all days within the gaps.

```sql
token_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      token_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  )
```

## Daily Price of Given Token

This CTE calculates the average daily price of the token.

```sql
token_daily_prices AS (
    SELECT
      er.decimals,
      DATE_TRUNC('day', hour) AS day,
      AVG(dx.median_price) AS price
    FROM
      dex.prices dx
      JOIN tokens.erc20 er ON er.contract_address = {{token_address}}
    WHERE
      dx.contract_address = {{token_address}}
      AND er.blockchain = '{{chain}}'
    GROUP BY
      er.decimals,
      DATE_TRUNC('day', hour)
  )
```

## Holders in different ranges

This CTE calculates the number of token holders in various value ranges.

```sql
token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 0 AND 100 THEN address END) AS "0-100",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 100 AND 1000 THEN address END) AS "100-1000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 1000 AND 10000 THEN address END) AS "1000-10000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) BETWEEN 10000 AND 100000 THEN address END) AS "10000-100000",
      COUNT(CASE WHEN (balance * p.price / POWER(10, p.decimals)) > 100000 THEN address END) AS "100000+"
    FROM
      token_balance_all_days AS b
      LEFT JOIN token_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
```

## Final Select

The final SELECT statement retrieves the daily counts of token holders within specified USD value ranges, replacing any null values with zero, and orders the results by date.

```sql
SELECT
  htv_token."Date" AS "Date",
  COALESCE(htv_token."0-100", 0) AS "0-100",
  COALESCE(htv_token."100-1000", 0) AS "100-1000",
  COALESCE(htv_token."1000-10000", 0) AS "1000-10000",
  COALESCE(htv_token."10000-100000", 0) AS "10000-100000",
  COALESCE(htv_token."100000+", 0) AS "100000+"
FROM
  token_holders_with_token_value htv_token
ORDER BY
  1
```

## Tables used

- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)
- dex.prices (This table loads the prices of tokens from the dex.trades table. This helps for missing tokens from the prices.usd table. Made by @henrystats. Present in the spellbook of dune analytics [Spellbook-Dex-Prices](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_schema.yml))
