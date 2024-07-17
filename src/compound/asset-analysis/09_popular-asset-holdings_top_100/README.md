# About

The table shows the given token's holdings and its USD balance of the Top 100 holders of the given token and blockchain, excluding centralized and decentralized exchanges. It also retrieves the holdings and USD balance of popular tokens for the top 100 token holders, including UNI, LINK, USDC, USDT, LDO.

# Graph

![popularAssetHoldings](popular-asset-holdings.png)
![popularAssetHoldings2](popular-asset-holdings-2.png)

# Relevance

This analysis is relevant for understanding asset distribution and the investment behavior of top holders. By excluding centralized and decentralized exchanges, the query focuses on significant individual holders, providing a clearer picture of actual asset ownership and usage. Key insights include identifying major holders of the token and assessing the diversity of holdings .among top holders

# Query Explanation

Identifies the top 100 holders of a specific token, excluding known exchange addresses, and their holdings in other key tokens (LINK, UNI, LDO, USDC, USDT) for portfolio diversification insights.

Price CTE calculates the average price of the specified token as well as of the popular tokens and get their decimals and symbol as well for further calculation, grouping them by their contract addresses, symbols, and decimals.

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
      erc.contract_address IN (
        0x514910771af9ca656af840dff83e8264ecf986ca, -- LINK token
        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984, -- UNI token
        0x5a98fcbea516cf06857215779fd812ca3bef1b32, -- LDO token
        0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, -- USDC token
        0xdAC17F958D2ee523a2206206994597C13D831ec7, -- USDT token
        {{token_address}}
      )
      AND dex.token_address IN (
        0x514910771af9ca656af840dff83e8264ecf986ca,
        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
        0x5a98fcbea516cf06857215779fd812ca3bef1b32,
        0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
        0xdAC17F958D2ee523a2206206994597C13D831ec7,
        {{token_address}}
      )
      AND erc.blockchain = '{{chain}}'
    GROUP BY
      erc.symbol,
      erc.decimals,
      erc.contract_address
  )
```

Token raw CTE calculates the net amount of tokens held by each address by summing up incoming and outgoing transfers, for the given token.

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

Token distribution CTE calculates the token holdings and their USD value for each address of the given token.

```sql
token_distribution AS (
    SELECT
      address,
      SUM(amount / POWER(10, decimals)) AS holding,
      SUM(amount * price / POWER(10, decimals)) AS holding_usd
    FROM
      price,
      token_raw
      LEFT JOIN contracts.contract_mapping c ON address = CAST(c.contract_address AS VARCHAR)
    WHERE
      price.contract_address = {{token_address}}
      and address not in (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dEaD'
      )
      AND (
        c.contract_address IS NULL
        OR c.contract_project = 'Gnosis Safe'
      )
    GROUP BY
      address
  )
```

DEX CEX addresses CTE retrieves a list of known centralized and decentralized exchange addresses for the given blockchain. DEX addresses comes from two tables as `dex.addresses` doesnt have all the DEX addresses so union them with addresses from `dex.trades`.

```sql
dex_cex_addresses AS (
    SELECT
      CAST(address as Varchar) AS address
    FROM
      cex.addresses
    WHERE
      blockchain = '{{chain}}'
    UNION ALL
    SELECT
      address
    FROM
      (
        SELECT
          CAST(address as Varchar) AS address
        FROM
          dex.addresses
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
        UNION ALL
        SELECT
          CAST(project_contract_address as Varchar) AS address
        FROM
          dex.trades
        WHERE
          blockchain = '{{chain}}'
        GROUP BY
          1
      )
  )
```

Top 100 token holders CTE filters out exchange addresses and selects the top 100 holders by token holding.

```sql
top_100_token_holders AS (
    SELECT
      td.address,
      td.holding AS token_holding,
      td.holding_usd AS token_holding_usd
    FROM
      token_distribution td
    WHERE
      td.address not in (
        select distinct
          address
        from
          dex_cex_addresses
      )
    ORDER BY
      td.holding DESC
    LIMIT
      100
  )
```

Other tokens raw CTE retrieves the net amount of other popular tokens held by each address.

```sql
other_tokens_raw AS (
    SELECT
      CAST("from" AS VARCHAR) AS address,
      contract_address,
      SUM(CAST(value AS DOUBLE) * -1) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address IN (
        0x514910771af9ca656af840dff83e8264ecf986ca,
        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
        0x5a98fcbea516cf06857215779fd812ca3bef1b32,
        0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
        0xdAC17F958D2ee523a2206206994597C13D831ec7
      )
    GROUP BY
      1,
      2
    UNION ALL
    SELECT
      CAST("to" AS VARCHAR) AS address,
      contract_address,
      SUM(CAST(value AS DOUBLE)) AS amount
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address IN (
        0x514910771af9ca656af840dff83e8264ecf986ca,
        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
        0x5a98fcbea516cf06857215779fd812ca3bef1b32,
        0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
        0xdAC17F958D2ee523a2206206994597C13D831ec7
      )
    GROUP BY
      1,
      2
  )
```

Other tokens distribution CTE calculates the balance of other popular tokens held by each address.

```sql
other_tokens_distribution AS (
    SELECT
      address,
      p.contract_address,
      SUM(amount / POWER(10, decimals)) AS balance
    FROM
      price p,
      other_tokens_raw otr
    WHERE
      p.contract_address = otr.contract_address
    GROUP BY
      address,
      p.contract_address,
      decimals
  )
```

All balances CTE combines all balances and token holdings for the top 100 token holders, including the given token and popular tokens.

```sql
all_balances AS (
    SELECT
      u.address,
      u.token_holding,
      u.token_holding_usd,
      ot.contract_address,
      ot.balance,
      tp.price
    FROM
      top_100_token_holders u
      LEFT JOIN other_tokens_distribution ot ON u.address = ot.address
      LEFT JOIN price tp ON ot.contract_address = tp.contract_address
  )
```

Finally aggregates the token holdings and their USD value for each address, as well as their holdings of other popular tokens.

```sql
SELECT
  ab.address,
  ab.token_holding,
  ab.token_holding_usd,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x514910771af9ca656af840dff83e8264ecf986ca THEN ab.balance
      END
    ),
    0
  ) AS link_holding,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x514910771af9ca656af840dff83e8264ecf986ca THEN ab.balance * ab.price
      END
    ),
    0
  ) AS link_holding_usd,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984 THEN ab.balance
      END
    ),
    0
  ) AS uni_holding,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984 THEN ab.balance * ab.price
      END
    ),
    0
  ) AS uni_holding_usd,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x5a98fcbea516cf06857215779fd812ca3bef1b32 THEN ab.balance
      END
    ),
    0
  ) AS ldo_holding,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0x5a98fcbea516cf06857215779fd812ca3bef1b32 THEN ab.balance * ab.price
      END
    ),
    0
  ) AS ldo_holding_usd,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN ab.balance
      END
    ),
    0
  ) AS usdc_holding,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN ab.balance * ab.price
      END
    ),
    0
  ) AS usdc_holding_usd,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN ab.balance
      END
    ),
    0
  ) AS usdt_holding,
  COALESCE(
    MAX(
      CASE
        WHEN ab.contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN ab.balance * ab.price
      END
    ),
    0
  ) AS usdt_holding_usd
FROM
  all_balances ab
GROUP BY
  ab.address,
  ab.token_holding,
  ab.token_holding_usd
ORDER BY
  ab.token_holding DESC;
```

**Hardcoded addresses**

- [0x514910771af9ca656af840dff83e8264ecf986ca](https://etherscan.io/address/0x514910771af9ca656af840dff83e8264ecf986ca): LINK token address
- [0x1f9840a85d5af5bf1d1762f925bdaddc4201f984](https://etherscan.io/address/0x1f9840a85d5af5bf1d1762f925bdaddc4201f984): UNI token address
- [0x5a98fcbea516cf06857215779fd812ca3bef1b32](https://etherscan.io/address/0x5a98fcbea516cf06857215779fd812ca3bef1b32): LDO token address
- [0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48](https://etherscan.io/address/0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48): USDC token address
- [0xdAC17F958D2ee523a2206206994597C13D831ec7](https://etherscan.io/address/0xdAC17F958D2ee523a2206206994597C13D831ec7): USDT token address
- [0x0000000000000000000000000000000000000000](https://etherscan.io/address/0x0000000000000000000000000000000000000000): This address is not owned by any user, is often associated with token burn & mint/genesis events and used as a generic null address.
- [0x000000000000000000000000000000000000dEaD](https://etherscan.io/address/0x000000000000000000000000000000000000dEaD): This address is commonly used by projects to burn tokens (reducing total supply).

## Tables used

- dex.prices_latest (Curated dataset contains token addresses and their USD price. Made by @bernat. Present in the spellbook of dune analytics [Spellbook-Dex-PricesLatest](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_prices_latest.sql))
- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)
- dex.addresses (Curated dataset contains known decentralised exchange addresses. Made by @rantum. Present in the spellbook of dune analytics [Spellbook-CEX](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_schema.yml))
- dex.trades (Curated dataset contains DEX trade info like taker and maker. Present in spellbook of dune analytics [Spellbook-Dex-Trades](https://github.com/duneanalytics/spellbook/blob/main/models/_sector/dex/trades/dex_trades.sql))
- cex.addresses (Curated dataset contains all CEX-tied addresses identified. Made by @hildobby. Present in the spellbook of dune analytics [Spellbook-CEX](https://github.com/duneanalytics/spellbook/blob/main/models/cex/cex_addresses.sql))
- contracts.contract_mapping (Curated dataset contains mapping of contracts to its creators and names on EVM chains.)

## Alternative Choices
