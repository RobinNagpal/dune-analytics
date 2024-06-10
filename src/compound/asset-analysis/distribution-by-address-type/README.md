# About

The graph shows participation of different entities in the market and how much percentage of the total asset they hold.

# Graph

![distributionByAddressType](distribution-by-address-type.png)

# Relevance

This graph is relevant to understand the distribution of the asset among different entities. It helps to understand the flow of the asset in the market by different entities and the extent of decentralization of the asset. It also helps to understand the activity of the asset in the market.

For example:

- Large holding by exchange addresses shows active trading
- Holdings by smart contracts shows token’s utility
- Large holdings by individuals show broad adoption and support of the investors

# Query Explanation

This query calculates the distribution of token holdings by categorizing addresses into different types and summing their holdings. It considers various types of addresses like exchanges, smart contracts, multi-sig wallets, venture capital funds, and individual addresses. By joining price information and raw transfer data, it calculates the total holdings and their USD value, then filters and groups these holdings by address type.

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

Fund_address CTE creates a list of addresses corresponding to specific funds

```sql
fund_address AS (
    SELECT
        address
    FROM
        (
            VALUES
                (0x820fb25352bb0c5e03e07afc1d86252ffd2f0a18, 'Paradigm'),
                (0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0, 'Jump Trading')
        ) AS t (address, name)
    UNION ALL
    SELECT DISTINCT
        address
    FROM
        labels.funds
)
```

Finally categorize each of the address into one of the several types, filtering out the null address:

- CEX: Centralized Exchange addresses.
- DEX: Decentralized Exchange addresses.
- Multi-Sig Wallet: Addresses identified as multi-signature wallets.
- Other Smart Contracts: Addresses identified as smart contracts but not as DEX, Multi-Sig, or Fund addresses
- VCs/Fund: Addresses identified as belonging to venture capital or funds.
- Individual Address: All other addresses.

Groups by address and type to sum their holdings and value.
Filters out addresses with a value of less than 1.
Aggregates the total holdings by address type.

```sql
SELECT
    type,
    SUM(amount) AS total_holdings
FROM
    (
        SELECT
            address,
            CASE
                WHEN address IN (
                    SELECT DISTINCT address FROM cex_evms.addresses
                )
                OR address IN (
                    SELECT DISTINCT address FROM query_2296923
                ) THEN 'CEX'
                WHEN address IN (
                    SELECT DISTINCT project_contract_address FROM dex.trades
                ) THEN 'DEX'
                WHEN address IN (
                    SELECT DISTINCT address FROM safe.safes_all
                ) THEN 'Multi-Sig Wallet'
                WHEN address IN (
                    SELECT DISTINCT address FROM {{Blockchain}}.creation_traces
                )
                AND address NOT IN (
                    SELECT DISTINCT project_contract_address FROM dex.trades
                )
                AND address NOT IN (
                    SELECT DISTINCT address FROM safe.safes_all
                )
                AND address NOT IN (
                    SELECT DISTINCT address FROM fund_address
                ) THEN 'Other Smart Contracts'
                WHEN address IN (
                    SELECT DISTINCT address FROM fund_address
                ) THEN 'VCs/Fund'
                ELSE 'Individual Address'
            END AS type,
            SUM(amount / POWER(10, decimals)) AS amount,
            SUM(amount * price / POWER(10, decimals)) AS value
        FROM
            price,
            raw
        WHERE
            address <> 0x0000000000000000000000000000000000000000
        GROUP BY
            address,
            type
    ) a
WHERE
    value > 1
GROUP BY
    type;
```

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)
- labels.funds (Curated dataset contains labels of known funds addresses across chains. Made by @soispoke. Present in the spellbook of dune analytics [Spellbook-Labels-Funds](https://github.com/duneanalytics/spellbook/blob/main/models/labels/addresses/institution/identifier/funds/labels_funds.sql))
- cex_evms.addresses (Curated dataset with centralized exchange names and addresses. Origin unknown)
- query_2296923 (returns table with exchange names and their addresses. Uses hardcoded values union with `dune_upload.okx_por_evm` table. [Query-2296923](https://dune.com/queries/2296923))
- dex.trades (Curated dataset contains DEX trade info like taker and maker. Present in spellbook of dune analytics [Spellbook-Dex-Trades](https://github.com/duneanalytics/spellbook/blob/main/models/_sector/dex/trades/dex_trades.sql))
- safe.safes_all (Curated dataset that lists all Safes across chains. Present in the spellbook of dune analytics [Spellbook-Safe-SafesAll](https://github.com/duneanalytics/spellbook/blob/main/models/safe/safe_safes_all.sql))
- {{Blockchain}}.creation_traces (Raw data contains tx hash, address and code.)

## Alternative Choices
