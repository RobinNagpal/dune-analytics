# About

This query retrieves information about the liquidity pools on Uniswap for a specific token, including details about the dex's projects, versions, token holdings, and their USD value.

# Graph

![assetsAvailabilityOnPools](assets-availability-pool.png)

# Relevance

Understanding the distribution of assets in DEX pools is essential for assessing the market penetration and liquidity of a token. This query helps in identifying:
- The DEXs on which the token is available, indicating its popularity and acceptance.
- The token's liquidity in different pools, providing insights into its market activity.
- The pairings of tokens in pools, revealing how the token is being traded and utilized.

# Query Explanation

This query identifies the DEX pools on a specified blockchain and extracts relevant information such as the DEX name, pool address, token symbols, and their holdings. By joining with token data, it also calculates the current price of the specified token and its holdings in USD.

Extracts DEX pool addresses along with the DEX name, version, and token addresses.

```sql
WITH dex_pool_addresses AS (
    SELECT
        CAST(pool as Varchar) AS address,
        project,
        version,
        token0,
        token1
    FROM
        dex.pools
    WHERE
        blockchain = '{{chain}}'
    GROUP BY
        1,
        2,
        3,
        4,
        5
)
```

Retrieves the current price of the specified token.

```sql
price AS (
    SELECT
        erc.symbol,
        erc.decimals,
        erc.contract_address,
        AVG(dex.token_price_usd) as price
    FROM
        dex.prices_latest dex
        JOIN tokens.erc20 erc ON dex.token_address = erc.contract_address
    WHERE
        erc.contract_address = {{token_address}}
        AND dex.token_address = {{token_address}}
        AND erc.blockchain = '{{chain}}'
    GROUP BY
        erc.symbol,
        erc.decimals,
        erc.contract_address
)
```

Calculates the net amount of tokens held by each pool by summing up incoming and outgoing transfers.

```sql
token_raw AS (
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

Aggregates the token holdings and their USD values.

```sql
token_distribution AS (
    SELECT
        address,
        SUM(amount / POWER(10, decimals)) AS holding,
        SUM(amount * price / POWER(10, decimals)) AS holding_usd
    FROM
        price,
        token_raw
    WHERE
        price.contract_address = {{token_address}}
    GROUP BY
        address
)
```

Combines the token distribution with pool addresses to get the token holding and price in usd.

```sql
pool_token_holding AS (
    SELECT
        td.address,
        da.project,
        da.version,
        da.token0,
        da.token1,
        td.holding AS token_holding,
        td.holding_usd AS token_holding_usd
    FROM
        token_distribution td
        JOIN dex_pool_addresses da ON td.address = da.address
    WHERE
        td.holding > 0
)
```

Joins the Pool token holdings with token data to get the symbols of the tokens in the pools.

```sql
token_symbols AS (
    SELECT
        dth.address,
        dth.project,
        dth.version,
        dth.token_holding,
        dth.token_holding_usd,
        dth.token0,
        dth.token1,
        t0.symbol AS token0_symbol,
        t1.symbol AS token1_symbol
    FROM
        pool_token_holding dth
        JOIN tokens.erc20 t0 ON dth.token0 = t0.contract_address
        JOIN tokens.erc20 t1 ON dth.token1 = t1.contract_address
)
```

Combines the symbols of the tokens and returns the desired columns.

```sql
SELECT DISTINCT
    project,
    version,
    address AS pool_address,
    CONCAT(token0_symbol, '/', token1_symbol) AS pool_symbol,
    token0,
    token1,
    token_holding,
    token_holding_usd
FROM
    token_symbols
ORDER BY
    token_holding DESC;
```

## Tables used

- dex.pools (Curated dataset contains DEX pools on all chains across all contracts and versions. Made by @hildobby)
- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)


