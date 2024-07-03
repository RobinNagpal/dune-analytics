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
  transaction_fees AS (
    SELECT
      DATE(evt_block_time) AS day,
      token_name,
      CAST(txs.gas_used AS BIGINT) * CAST(txs.gas_price AS BIGINT) / POWER(10, 18) AS fee_eth
    FROM
      erc20_{{chain}}.evt_Transfer evt
      JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
      JOIN token_details td ON evt.contract_address = td.token_address
    WHERE
      evt_block_time >= CURRENT_DATE - INTERVAL '{{duration_window_days}}' day
  ),
  average_fees AS (
    SELECT
      day,
      token_name,
      AVG(fee_eth) AS average_transaction_fee_eth
    FROM
      transaction_fees
    GROUP BY
      day,
      token_name
  )
SELECT
  a.day,
  a.average_transaction_fee_eth AS average_transaction_fee_eth_main,
  u.average_transaction_fee_eth AS average_transaction_fee_eth_uni,
  l.average_transaction_fee_eth AS average_transaction_fee_eth_link
FROM
  average_fees a
  LEFT JOIN average_fees u ON a.day = u.day
  AND u.token_name = 'uni'
  LEFT JOIN average_fees l ON a.day = l.day
  AND l.token_name = 'link'
WHERE
  a.token_name = 'main_token'
ORDER BY
  a.day;