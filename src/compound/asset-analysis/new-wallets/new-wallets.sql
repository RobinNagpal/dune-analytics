WITH
  new_transfers AS (
    SELECT
      to AS wallet,
      MIN(evt_block_time) AS time
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
    GROUP BY
      1
  ),
  new_wallets AS (
    SELECT
      COUNT(*) AS new_wallets,
      DATE_TRUNC('day', time) AS day
    FROM
      new_transfers
    GROUP BY
      2
  ),
  prices AS (
    SELECT
      AVG(price) AS price,
      DATE_TRUNC('day', minute) AS day
    FROM
      prices.usd_forward_fill
    WHERE
      contract_address = {{token_address}}
      AND blockchain = '{{chain}}'
      AND minute > NOW() - INTERVAL '{{day}}' day
    GROUP BY
      2
  )
SELECT
  a.new_wallets AS new_wallets_created,
  SUM(a.new_wallets) OVER (
    ORDER BY
      a.day NULLS FIRST
  ) AS cum_new_wallets,
  a.day AS day,
  b.price AS token_price
FROM
  new_wallets AS a
  LEFT JOIN prices AS b ON a.day = b.day
WHERE
  a.day > NOW() - INTERVAL '{{day}}' day
ORDER BY
  a.day DESC


-- from start till now

-- WITH
--   new_transfers AS (
--     SELECT
--       to AS wallet,
--       MIN(evt_block_time) AS time
--     FROM
--       erc20_{{chain}}.evt_Transfer
--     WHERE
--       contract_address = {{token_address}}
--     GROUP BY
--       1
--   ),
--   new_wallets AS (
--     SELECT
--       COUNT(*) AS new_wallets,
--       DATE_TRUNC('day', time) AS day
--     FROM
--       new_transfers
--     GROUP BY
--       2
--   ),
--   prices AS (
--     SELECT
--       AVG(price) AS price,
--       DATE_TRUNC('day', minute) AS day
--     FROM
--       prices.usd_forward_fill
--     WHERE
--       contract_address = {{token_address}}
--       AND blockchain = '{{chain}}'
--     GROUP BY
--       2
--   )
-- SELECT
--   a.new_wallets AS new_wallets_created,
--   SUM(a.new_wallets) OVER (
--     ORDER BY
--       a.day NULLS FIRST
--   ) AS cum_new_wallets,
--   a.day AS day,
--   b.price AS token_price
-- FROM
--   new_wallets AS a
--   LEFT JOIN prices AS b ON a.day = b.day
-- ORDER BY
--   a.day DESC;