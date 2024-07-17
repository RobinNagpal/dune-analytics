# About

This shows basic info about an asset like its symbol, current price in USD, market cap and total supply to give a quick glance of the asset's current situation in the market.

# Graph

![assetInfo](asset-info.png)

# Relevance

Market cap is crucial for assessing the size, growth, and market share of a token relative to others in the market. Understanding the circulating supply is essential for evaluating the liquidity of a token, which can affect its volatility and price. Asset's current price (in USD) shows asset's worth in the market upon which users usually base their decision of buying the asset and in what quantity.

# Query Explanation

This query gets the token symbol, its market current price in USD, calculates the market capitalization and total supply in the market.

Dex Price CTE retrieves the latest USD price of the token from a latest price data source.

```sql
dex_price AS (
    SELECT
      symbol AS dex_symbol,
      decimals AS dex_decimals,
      price AS dex_price
    FROM
      prices.usd_latest
    WHERE
      contract_address = {{token_address}}
    ORDER BY
      minute DESC
    LIMIT
      1
  )
```

Minted Supply CTE calculates the total amount of tokens minted by summing up the values of transfers where the from address is 0x0000000000000000000000000000000000000000.

```sql
minted_supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS minted_amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "from" = 0x0000000000000000000000000000000000000000
  )
```

Burned Supply CTE calculates the total amount of tokens burned by summing up the values of transfers where the to address is 0x0000000000000000000000000000000000000000.

```sql
burned_supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS burned_amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "to" IN (0x0000000000000000000000000000000000000000)
  )
```

**Hardcoded addresses**
- 0x0000000000000000000000000000000000000000: This address is not owned by any user, is often associated with token burn & mint/genesis events and used as a generic null address

Total Supply CTE calculates the total amount of tokens that have been transferred

```sql
total_supply AS (
    SELECT
      (
        COALESCE(m.minted_amount, 0) - COALESCE(b.burned_amount, 0)
      ) AS net_supply
    FROM
      minted_supply m
      CROSS JOIN burned_supply b
  )
```

This CTE combines data from the dex_price and total_supply tables, selecting the token symbol, decimals, and price from dex_price, along with the net supply from total_supply.

```sql
aggregated_data AS (
    SELECT
      d.dex_symbol AS symbol,
      d.dex_decimals AS decimals,
      d.dex_price AS price,
      t.net_supply
    FROM
      dex_price d,
      total_supply t
  )
```

Finally calculates the market capitalization and circulating supply of the token. Market cap by multiplying the total amount of tokens by the average token price and circulating supply by converting the raw token amounts from the smallest unit to a readable format by adjusting for decimal places.

```sql
SELECT
  symbol,
  price,
  net_supply / POWER(10, decimals) AS total_supply,
  (net_supply / POWER(10, decimals)) * price AS market_cap
FROM
  aggregated_data;
```

## Tables used

- prices.usd_latest (Curated dataset which contains latest prices table across blockchains. Made by @hildobby and @0xRob)
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

## Alternative Choices



## TODO
1. Whenever we have hardcoded addresses or any other hardcoded information, please explain each hardcoded information. Here we have the following hardcoded addresses
    ```
    ("from" = 0x0000000000000000000000000000000000000000 OR
          "to" IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3))
      )
    ```
    Please explain each of these



2. We should try to display some of the missing fields from below
    ![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/16eb1f92-8fce-46a5-b351-7dda74b4421a)


3. We should also show last three month price compared with ETH. So plot both, the asset price and also the ETH price on the same chart
    ![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/7d0a8b19-c020-4950-87a9-5100adf7e45d)

4. We should also try to get some information about the presence of these assets on some of the exchanges also. This is imporant becuase, usually after liquidation, the liquidator uses one of these exchange
    ![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/7941a1e6-657c-4877-8386-0461e88fc545)

