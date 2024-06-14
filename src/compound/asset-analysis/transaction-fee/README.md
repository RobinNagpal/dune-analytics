# About

This shows average transaction fee (in ETH) of the token for the past 24 hours for transaction cost analysis influencing smart contract execution conditions.

# Graph

![assetInfo](asset-info.png)

# Relevance

- Transaction Cost Analysis: It helps users and developers estimate the cost of using the token for transfers, smart contract interactions, or other operations.
- Network Congestion and Gas Prices: High transaction fees may indicate network congestion, increasing demand for block space, or changes in the network's gas price policies. This analysis helps stakeholders understand network conditions that could affect transaction costs.
- Financial Planning and Operational Costs: For businesses and services that rely on token transfers or smart contract interactions (like DApps, exchanges, wallets), understanding transaction fees is essential for financial planning and managing operational costs.

# Query Explanation

This query calculates the average transaction fee from the transactions of the token for the past 24 hours.

Transaction fee CTE compute the transaction fees in ETH for each relevant transaction involving the given token. For each transaction, the fee in ETH is calculated by multiplying the gas_used by gas_price and then dividing by 10^18 to convert from wei to ETH.

```sql
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
```

Finally calculate the average transaction fee in ETH for the collected transactions.

```sql
SELECT
  AVG(fee_eth) AS average_transaction_fee_eth
FROM
  transaction_fees;
```

## Tables used

- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

## Alternative Choices
