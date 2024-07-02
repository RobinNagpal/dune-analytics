WITH
  transaction_fees AS (
    SELECT
      txs.hash,
      CAST(txs.gas_used AS BIGINT) * CAST(txs.gas_price AS BIGINT) / POWER(10, 18) AS fee_eth
    FROM
      erc20_{{chain}}.evt_Transfer evt
      JOIN {{chain}}.transactions txs ON evt.evt_tx_hash = txs.hash
    WHERE
      evt.contract_address = {{token_address}}
      AND evt_block_time >= CURRENT_DATE - INTERVAL '1' day
  )
SELECT
  AVG(fee_eth) AS average_transaction_fee_eth
FROM
  transaction_fees;