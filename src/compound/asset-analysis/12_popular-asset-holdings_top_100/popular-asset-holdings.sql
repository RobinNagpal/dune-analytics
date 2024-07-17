WITH
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
  ),
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
  ),
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
  ),
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
  ),
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
  ),
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
  ),
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
  ),
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