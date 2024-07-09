select
  count(*) as trade_count,
  project
from
  dex.trades
where
  (
    token_bought_address = {{token_address}}
    or token_sold_address = {{token_address}}
  )
  AND block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
group by
  project
ORDER BY
  trade_count DESC
LIMIT
  6