WITH
  decimals_info_token AS (
    SELECT
      decimals
    FROM
      tokens.erc20
    WHERE
      contract_address = {{token_address}}
  ),
  decimals_info_uni AS (
    SELECT
      decimals
    FROM
      tokens.erc20
    WHERE
      contract_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
  ),
  decimals_info_link AS (
    SELECT
      decimals
    FROM
      tokens.erc20
    WHERE
      contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
  ),
  value_transfers_token AS (
    SELECT
      b.value / POWER(10, d.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS b
      INNER JOIN ethereum.transactions AS tx ON tx.hash = b.evt_tx_hash
      CROSS JOIN decimals_info_token d
    WHERE
      b.contract_address = {{token_address}}
      AND b."from" = 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
      - d.value / POWER(10, e.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS d
      INNER JOIN ethereum.transactions AS tx ON tx.hash = d.evt_tx_hash
      CROSS JOIN decimals_info_token e
    WHERE
      d.contract_address = {{token_address}}
      AND d."to" = 0x0000000000000000000000000000000000000000
  ),
  value_transfers_uni AS (
    SELECT
      b.value / POWER(10, d.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS b
      INNER JOIN ethereum.transactions AS tx ON tx.hash = b.evt_tx_hash
      CROSS JOIN decimals_info_uni d
    WHERE
      b.contract_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
      AND b."from" = 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
      - d.value / POWER(10, e.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS d
      INNER JOIN ethereum.transactions AS tx ON tx.hash = d.evt_tx_hash
      CROSS JOIN decimals_info_uni e
    WHERE
      d.contract_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
      AND d."to" = 0x0000000000000000000000000000000000000000
  ),
  value_transfers_link AS (
    SELECT
      b.value / POWER(10, d.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS b
      INNER JOIN ethereum.transactions AS tx ON tx.hash = b.evt_tx_hash
      CROSS JOIN decimals_info_link d
    WHERE
      b.contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
      AND b."from" = 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
      - d.value / POWER(10, e.decimals) AS value
    FROM
      erc20_ethereum.evt_Transfer AS d
      INNER JOIN ethereum.transactions AS tx ON tx.hash = d.evt_tx_hash
      CROSS JOIN decimals_info_link e
    WHERE
      d.contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
      AND d."to" = 0x0000000000000000000000000000000000000000
  ),
  token_total_supply AS (
    SELECT
      SUM(value) AS total_supply
    FROM
      value_transfers_token
  ),
  uni_total_supply AS (
    SELECT
      SUM(value) AS total_supply
    FROM
      value_transfers_uni
  ),
  link_total_supply AS (
    SELECT
      SUM(value) AS total_supply
    FROM
      value_transfers_link
  ),
  token_transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          CAST(value AS DECIMAL (38, 0)) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          (-1) * (CAST(value AS DECIMAL (38, 0))) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
      ) AS t
    GROUP BY
      1,
      2,
      3
  ),
  uni_transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          CAST(value AS DECIMAL (38, 0)) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr
        WHERE
          contract_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          (-1) * (CAST(value AS DECIMAL (38, 0))) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr
        WHERE
          contract_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
      ) AS t
    GROUP BY
      1,
      2,
      3
  ),
  link_transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          CAST(value AS DECIMAL (38, 0)) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr
        WHERE
          contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          (-1) * (CAST(value AS DECIMAL (38, 0))) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr
        WHERE
          contract_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
      ) AS t
    GROUP BY
      1,
      2,
      3
  ),
  token_balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(amount) OVER (
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
      token_transfers AS t
  ),
  uni_balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(amount) OVER (
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
      uni_transfers AS t
  ),
  link_balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(amount) OVER (
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
      link_transfers AS t
  ),
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
  ),
  token_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(
        balance / TRY_CAST(POWER(10, di.decimals) AS DOUBLE)
      ) AS balance
    FROM
      token_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
      CROSS JOIN decimals_info_token di
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  uni_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(
        balance / TRY_CAST(POWER(10, di.decimals) AS DOUBLE)
      ) AS balance
    FROM
      uni_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
      cross join decimals_info_uni di
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  link_balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(
        balance / TRY_CAST(POWER(10, di.decimals) AS DOUBLE)
      ) AS balance
    FROM
      link_balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
      cross join decimals_info_link di
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.00001 THEN b.address
        END
      ) AS "Holders with Tokens > 0.001%"
    FROM
      token_balance_all_days AS b
      CROSS JOIN token_total_supply ts
    WHERE
      balance > 0
    GROUP BY
      b.day,
      ts.total_supply
  ),
  uni_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.00001 THEN b.address
        END
      ) AS "Uni Holders with Tokens > 0.001%"
    FROM
      uni_balance_all_days AS b
      CROSS JOIN uni_total_supply AS ts
    WHERE
      balance > 0
    GROUP BY
      b.day,
      ts.total_supply
  ),
  link_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN b.balance > ts.total_supply * 0.00001 THEN b.address
        END
      ) AS "Link Holders with Tokens > 0.001%"
    FROM
      link_balance_all_days AS b
      CROSS JOIN link_total_supply AS ts
    WHERE
      balance > 0
    GROUP BY
      b.day,
      ts.total_supply
  )
SELECT
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date") AS "Date",
  COALESCE(htv_token."Holders with Tokens > 0.001%", 0) AS "Token Holders",
  COALESCE(htv_uni."Uni Holders with Tokens > 0.001%", 0) AS "UNI Holders",
  COALESCE(htv_link."Link Holders with Tokens > 0.001%", 0) AS "LINK Holders"
FROM
  token_holders_with_token_value htv_token
  FULL JOIN uni_holders_with_token_value htv_uni ON htv_token."Date" = htv_uni."Date"
  FULL JOIN link_holders_with_token_value htv_link ON htv_token."Date" = htv_link."Date"
ORDER BY
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date");