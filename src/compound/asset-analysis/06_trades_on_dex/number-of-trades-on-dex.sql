WITH
  given_token_trades AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      COUNT(*) AS given_token_trade_count
    FROM
      dex.trades
    WHERE
      (
        token_bought_address = {{token_address}}
        OR token_sold_address = {{token_address}}
      )
      AND block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
    GROUP BY
      1
  ),
  uni_token_trades AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      COUNT(*) AS uni_trade_count
    FROM
      dex.trades
    WHERE
      (
        token_bought_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
        OR token_sold_address = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984
      )
      AND block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
    GROUP BY
      1
  ),
  link_token_trades AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      COUNT(*) AS link_trade_count
    FROM
      dex.trades
    WHERE
      (
        token_bought_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
        OR token_sold_address = 0x514910771AF9Ca656af840dff83E8264EcF986CA
      )
      AND block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
    GROUP BY
      1
  )
SELECT
  COALESCE(gt.day, ut.day, lt.day) AS day,
  COALESCE(gt.given_token_trade_count, 0) AS given_token_trades,
  COALESCE(ut.uni_trade_count, 0) AS uni_token_trades,
  COALESCE(lt.link_trade_count, 0) AS link_token_trades
FROM
  given_token_trades gt
  FULL OUTER JOIN uni_token_trades ut ON gt.day = ut.day
  FULL OUTER JOIN link_token_trades lt ON gt.day = lt.day
  OR ut.day = lt.day
ORDER BY
  day;