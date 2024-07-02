# About

This query calculates the total gas used for transactions of the given token, integrating the data with token holdings to identify the top 100 token holders and their respective gas expenditures.

# Graph

# Relevance

Understanding the gas expenditure in relation to the token transactions is crucial for the cost dynamics and operating in the blockchain ecosystem with the given token. By analyzing this attribute for the top 100 token holders, we get insights into the extent of the activity of the largest stakeholders cause more gas used indicate more change, more things that were done in a single transaction.

- Cost of Transactions: By calculating the total gas used by the top holders, the query highlights the transactional costs these key stakeholders are incurring, which can be significant in networks with high gas prices.
- Network Efficiency and Scalability: High gas usage can indicate issues with network scalability or inefficiencies in how transactions are processed, which are vital considerations for network upgrades or for developers designing smart contracts.

# Query Explanation

This query calculates the top 100 token holders by their token holdings. It also computes the total gas fees spent by these holders and joins this data for a comprehensive view of the top token holders and their activities.

Token Details CTE retrieves its symbol and decimals

```sql
token_details AS (
    SELECT
      symbol,
      decimals
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

Total Supply CTE calculates the total supply of the token by summing up the amounts from the raw CTE

```sql
total_supply AS (
    SELECT
      SUM(amount / POWER(10, decimals)) AS total_supply
    FROM
      raw,
      token_details
  )
```

Distribution CTE calculates the total holdings.

```sql
distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding
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
      d.address,
      d.holding
    FROM
      distribution d
      CROSS JOIN total_supply ts
    ORDER BY
      d.holding DESC
    LIMIT
      100
  )
```

Gas Fees CTE calculates the total gas used by each address in token transfer transactions.

```sql
gas_fees AS (
    SELECT
      address,
      SUM(CAST(gas_used AS DOUBLE)) AS total_gas_used
    FROM
      (
        SELECT
          CAST(evt."from" AS VARCHAR) AS address,
          gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
        UNION ALL
        SELECT
          CAST(evt."to" AS VARCHAR) AS address,
          gas_used
        FROM
          erc20_{{chain}}.evt_Transfer evt
          JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
        WHERE
          evt.contract_address = {{token_address}}
      ) gas_tx
    GROUP BY
      address
  )
```

Finally joins holders and gas fee CTEs to get the address, their holdings and total gas used by each address.

```sql
SELECT
  t.address,
  t.holding,
  COALESCE(gf.total_gas_used, 0) AS total_gas_used
FROM
  top_100_holders t
  LEFT JOIN gas_fees gf ON t.address = gf.address
ORDER BY
  t.holding DESC;
```

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

## Alternative Choices
