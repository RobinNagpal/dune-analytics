WITH
  token_details AS (
    SELECT
      {{token_address}} AS token_address,
      'main_token' AS token_name
    UNION ALL
    SELECT
      0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984 AS token_address,
      'uni' AS token_name
    UNION ALL
    SELECT
      0x514910771AF9Ca656af840dff83E8264EcF986CA AS token_address,
      'link' AS token_name
  ),
  transaction_counts AS (
    SELECT
      DATE(evt_block_time) AS day,
      token_name,
      evt.evt_tx_hash
    FROM
      erc20_{{chain}}.evt_Transfer evt
      JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
      JOIN token_details td ON evt.contract_address = td.token_address
    WHERE
      evt_block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
  ),
  total_transactions AS (
    SELECT
      day,
      token_name,
      COUNT(evt_tx_hash) AS transaction_count
    FROM
      transaction_counts
    GROUP BY
      day,
      token_name
  )
SELECT
  a.day,
  a.transaction_count AS transaction_count_main,
  COALESCE(u.transaction_count, 0) AS transaction_count_uni,
  COALESCE(l.transaction_count, 0) AS transaction_count_link
FROM
  total_transactions a
  LEFT JOIN total_transactions u ON a.day = u.day AND u.token_name = 'uni'
  LEFT JOIN total_transactions l ON a.day = l.day AND l.token_name = 'link'
WHERE
  a.token_name = 'main_token'
ORDER BY
  a.day;
