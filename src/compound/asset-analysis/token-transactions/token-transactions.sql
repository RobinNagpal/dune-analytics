WITH
  price AS (
    SELECT
      erc.decimals,
      erc.contract_address,
      AVG(dex.token_price_usd) as price
    FROM
      dex.prices_latest dex
      JOIN tokens.erc20 erc ON dex.token_address = erc.contract_address
    WHERE
      erc.contract_address = {{token_address}}
      AND dex.token_address = {{token_address}}
      AND erc.blockchain = '{{chain}}'
    GROUP BY
      erc.decimals,
      erc.contract_address
  ),
  transactions AS (
    SELECT
      evt.evt_block_number AS block_number,
      evt.evt_block_time AS block_time,
      evt.evt_tx_hash AS transaction_hash,
      CAST(evt."from" AS VARCHAR) AS from_address,
      CAST(evt."to" AS VARCHAR) AS to_address,
      (evt.value / POWER(10, p.decimals)) AS tokens_transferred,
      (evt.value * p.price / POWER(10, p.decimals)) AS usd_value
    FROM
      erc20_{{chain}}.evt_Transfer evt
      JOIN price p ON evt.contract_address = p.contract_address
    WHERE
      evt.contract_address = {{token_address}}
    ORDER BY
      block_time DESC
    LIMIT
      100
  )
SELECT
  *
FROM
  transactions