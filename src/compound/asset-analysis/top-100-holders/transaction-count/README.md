# About

This query calculates the total transaction count for the top 100 holders of the given token. It calculates the total number of transactions for each holder, providing insights into how actively these key stakeholders are engaging in trading or transferring the token. This kind of analysis helps in identifying not only the major holders by balance but also those who are most active in the market.

# Graph

# Relevance

- Market Activity and Liquidity: Higher transaction counts from major holders can indicate robust trading activity and contribute to greater liquidity in the market.
- Holder Engagement and Market Influence: Active trading by major holders can significantly influence market prices and trends, especially in smaller or less liquid markets.

# Query Explanation

This query calculates the top 100 token holders by their token holdings, including their token values in USD and their respective percentages of the total supply. It also computes the total transaction count of these holders and joins this data for a comprehensive view of the top token holders and their activities.

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
      token_address = {{token_address}}
      AND contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
    GROUP BY
      1,
      2
  )
```

Raw CTE calculates the net amount of tokens held by each address by summing up incoming and outgoing transfers

```sql
raw AS (
    SELECT
      CAST("from" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
    UNION ALL
    SELECT
      CAST("to" AS VARCHAR) AS address,
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
  )
```

Distribution CTE calculates the total holdings and the percentage of total supply each address holds.

```sql
distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding,
      SUM(amount * price / POWER(10, decimals)) AS holding_usd,
      SUM(amount) / (
        SELECT
          SUM(amount)
        FROM
          raw
      ) AS percent_holdings
    FROM
      price,
      raw
    GROUP BY
      address
  )
```

Top 100 holders CTE selects the top 100 addresses by their token holdings.

```sql
top_100_holders AS (
    SELECT
      address,
      holding,
      holding_usd,
      percent_holdings
    FROM
      distribution
    ORDER BY
      holding DESC
    LIMIT
      100
  )
```

Transaction count CTE calculates the number of transactions for each address.

```sql
transaction_counts AS (
    SELECT
      address,
      COUNT(*) AS transaction_count
    FROM
      (
        SELECT
          CAST("from" AS VARCHAR) AS address
        FROM
          erc20_{{chain}}.evt_Transfer
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          CAST("to" AS VARCHAR) AS address
        FROM
          erc20_{{chain}}.evt_Transfer
        WHERE
          contract_address = {{token_address}}
      ) tx
    GROUP BY
      address
  )
```

Finally shows the addresses, their token holdings in both tokens and USD, their percentage of total holdings, and their transaction count, ordered by transaction count.

```sql
SELECT
  t.address,
  t.holding,
  t.holding_usd,
  t.percent_holdings,
  COALESCE(tc.transaction_count, 0) AS transaction_count
FROM
  top_100_holders t
  LEFT JOIN transaction_counts tc ON t.address = tc.address
ORDER BY
  transaction_count DESC;
```

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

## Alternative Choices
