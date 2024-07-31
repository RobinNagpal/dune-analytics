WITH
  transfers AS (
    SELECT
      evt_block_time,
      contract_address,
      "from",
      to,
      value
    FROM
      erc20_{{chain}}.evt_Transfer
    WHERE
      contract_address = {{token_address}}
      AND "from" <> to
  ),
  -- Calculate the balance for each address
  balances AS (
    SELECT
      contract_address,
      to AS address,
      SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      contract_address,
      to
    UNION ALL
    SELECT
      contract_address,
      "from" AS address,
      - SUM(value) AS balance
    FROM
      transfers
    GROUP BY
      contract_address,
      "from"
  ),
  -- Aggregate the balances to get the final balance for each address
  final_balances AS (
    SELECT
      contract_address,
      address,
      SUM(balance) AS balance
    FROM
      balances
    GROUP BY
      contract_address,
      address
  )
SELECT
  COUNT(*) AS distinct_holders
FROM
  final_balances
WHERE
  balance > 0