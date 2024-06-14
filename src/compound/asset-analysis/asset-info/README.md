# About

This shows basic info about an asset like its symbol, current price in USD, market cap and total supply to give a quick glance of the asset's current situation in the market.

# Graph

![assetInfo](asset-info.png)

# Relevance

Market cap is crucial for assessing the size, growth, and market share of a token relative to others in the market. Understanding the circulating supply is essential for evaluating the liquidity of a token, which can affect its volatility and price. Asset's current price (in USD) shows asset's worth in the market upon which users usually base their decision of buying the asset and in what quantity.

# Query Explanation

This query gets the token symbol, its market current price in USD, calculates the market capitalization and total supply in the market.

Dex Price CTE retrieves the average USD price of the token from a decentralized exchange (DEX) price data source.

```sql
dex_price AS (
    SELECT
      symbol AS dex_symbol,
      decimals AS dex_decimals,
      AVG(token_price_usd) AS dex_price
    FROM
      dex.prices_latest,
      tokens.erc20
    WHERE
      token_address = {{token_address}} AND
      contract_address = {{token_address}} AND
      blockchain = '{{chain}}'
    GROUP BY
      dex_symbol,
      dex_decimals
  )
```

Supply CTE calculates the total amount of tokens that have been transferred

```sql
supply AS (
    SELECT
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}} AND
      ("from" = 0x0000000000000000000000000000000000000000 OR
      "to" IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3))
  )
```

**Hardcoded addresses**
* 0x0000000000000000000000000000000000000000
* 0x000000000000000000000000000000000000dEaD
* 0xD15a672319Cf0352560eE76d9e89eAB0889046D3

Finally calculates the market capitalization and circulating supply of the token. Market cap by multiplying the total amount of tokens by the average token price and circulating supply by converting the raw token amounts from the smallest unit to a readable format by adjusting for decimal places.

```sql
SELECT
  dex_symbol AS symbol,
  dex_price AS price,
  SUM(amount * dex_price / POWER(10, dex_decimals)) AS market_cap,
  SUM(amount / POWER(10, dex_decimals)) AS circulating_supply
FROM
  supply,
  dex_price
GROUP BY
  dex_symbol,
  dex_price;
```

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

## Alternative Choices



## TODO
Whenever we have hardcoded addresses or any other hardcoded information, please explain each hardcoded information. Here we have the following hardcoded addresses
```
("from" = 0x0000000000000000000000000000000000000000 OR
      "to" IN (0x0000000000000000000000000000000000000000, 0x000000000000000000000000000000000000dEaD, 0xD15a672319Cf0352560eE76d9e89eAB0889046D3))
  )
```
Please explain each of these



We should try to display some of the missing fields from below
![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/16eb1f92-8fce-46a5-b351-7dda74b4421a)


We should also show last three month price compared with ETH. So plot both, the asset price and also the ETH price on the same chart
![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/7d0a8b19-c020-4950-87a9-5100adf7e45d)

We should also try to get some information about the presence of these assets on some of the exchanges also. This is imporant becuase, usually after liquidation, the liquidator uses one of these exchange
![image](https://github.com/RobinNagpal/dune-analytics/assets/745748/7941a1e6-657c-4877-8386-0461e88fc545)

