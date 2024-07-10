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

Token Decimals CTE retrieves the decimal precision of a specific ERC-20 token on a specified blockchain

```sql
token_decimals as (
    select
      decimals
    from
      tokens.erc20
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
    group by
      1
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

Groups by address and type to sum their holdings.
Filters out addresses with a holdings of less than 0.
Aggregates the total holdings by address type.

```sql
select
  type,
  sum(amount) as total_holdings
from
  (
    select
      address,
      case
        when address in (
          select distinct
            address
          from
            cex_evms.addresses
        )
        or address in (
          select distinct
            address
          from
            labels.cex_ethereum
        ) then 'CEX'
        when address in (
          select distinct
            project_contract_address
          from
            dex.trades
        ) then 'DEX'
        when address in (
          select distinct
            address
          from
            safe.safes_all
        ) then 'Multi-Sig Wallet'
        when address in (
          select distinct
            address
          from
            {{chain}}.creation_traces
        )
        and address not in (
          select distinct
            project_contract_address
          from
            dex.trades
        )
        and address not in (
          select distinct
            address
          from
            safe.safes_all
        )
        and address not in (
          select distinct
            address
          from
            fund_address
        ) then 'Other Smart Contracts'
        when address in (
          select distinct
            address
          from
            fund_address
        ) then 'VCs/Fund'
        else 'Individual Address'
      end as type,
      sum(amount / power(10, decimals)) as amount
    from
      token_decimals,
      raw
    where
      address NOT IN (
        0x0000000000000000000000000000000000000000,
        0x000000000000000000000000000000000000dEaD
      )
    group by
      1,
      2
  ) a
where
  amount > 0
group by
  1
```

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)
- labels.funds (Curated dataset contains labels of known funds addresses across chains. Made by @soispoke. Present in the spellbook of dune analytics [Spellbook-Labels-Funds](https://github.com/duneanalytics/spellbook/blob/main/models/labels/addresses/institution/identifier/funds/labels_funds.sql))
- cex_evms.addresses (Curated dataset contains all CEX-tied addresses identified across EVM chains. Present in the spellbook of dune analytics [Spellbook-CEX](https://github.com/duneanalytics/spellbook/blob/main/models/cex/cex_evms_addresses.sql))
- dex.trades (Curated dataset contains DEX trade info like taker and maker. Present in spellbook of dune analytics [Spellbook-Dex-Trades](https://github.com/duneanalytics/spellbook/blob/main/models/_sector/dex/trades/dex_trades.sql))
- safe.safes_all (Curated dataset that lists all Safes across chains. Present in the spellbook of dune analytics [Spellbook-Safe-SafesAll](https://github.com/duneanalytics/spellbook/blob/main/models/safe/safe_safes_all.sql))
- {{Blockchain}}.creation_traces (Raw data contains tx hash, address and code.)

## Alternative Choices
