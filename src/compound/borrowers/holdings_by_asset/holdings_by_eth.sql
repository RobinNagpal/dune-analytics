/*
 - Note down the list of asset addresses. Top 10 asset addresses
 - For each of the assets we will run this query to know how many borrowers hold how much each of the asset
 - So we write a separate query for each of the assets
 - Also display the price of the range of the asset.  So it should have two columns for the range of the asset
 
 */
-- with contract_list (address, name) as (
-- values 
-- (0x, ' ')
-- ),
-- WITH contract_list (address, name) as (
--     VALUES 
--     -- (0xdAC17F958D2ee523a2206206994597C13D831ec7, 'Tether USD(USDT)')
--     -- (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 'USDC'),
--     -- (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 'Lido Staked Ether(stETH)'),
--     -- (0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce, 'Shiba Inu(SHIB)'),
--     -- (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'Wrapped BTC(WBTC)'),
--     -- (0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, 'Wrapped ETH(WETH)'),
--     -- (0x514910771AF9Ca656af840dff83E8264EcF986CA, 'LINK'),
--     -- (0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D, 'UNI'),
--     -- (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0, 'MATIC'),
--     -- (0xF57e7e7C23978C3cAEC3C3548E3D615c346e79fF, 'IMX')
-- ),
with contract_list (address, name) as (
  values
    (0x, ' ')
),
eth_transfer_raw as (
  select
    "from" as address,
    (-1) * cast(value as decimal(38, 0)) as amount
  from
    ethereum.traces
  where
    call_type = 'call'
    and success = true
    and value > uint256 '0'
    and "from" is not null
    and "to" is not null
  union
  all
  select
    "to" as address,
    cast(value as decimal(38, 0)) as amount
  from
    ethereum.traces
  where
    call_type = 'call'
    and success = true
    and value > uint256 '0'
    and "from" is not null
    and "to" is not null
  union
  all
  select
    "from" as address,
    (-1) * cast(gas_price as double) * gas_used as amount
  from
    ethereum.transactions
  where
    success = true
),
eth_create_traces as (
  select
    address
  from
    ethereum.creation_traces
),
eth_balance as (
  select
    address,
    sum(amount) as balance_amount
  from
    eth_transfer_raw
  where
    address is not null -- exclude the null address data
    and address not in (
      select
        address
      from
        contract_list
    )
    and address not in (
      select
        address
      from
        eth_create_traces
    )
  group by
    1
  order by
    2 desc
),
balance_classification as (
  select
    address,
    balance_amount / 1e18 as eth_balance_amount,
    case
      when balance_amount = 0 then '0. 0 ETH'
      when balance_amount > 0
      and balance_amount < 0.01 * 1e18 then '1. (0, 0.01) ETH'
      when balance_amount >= 0.01 * 1e18
      and balance_amount < 0.1 * 1e18 then '2. [0.01, 0.1) ETH' --know that the data for 1 to 9 wiill be skewed because you're limiting
      when balance_amount >= 0.1 * 1e18
      and balance_amount < 1 * 1e18 then '3. [0.1, 1) ETH'
      when balance_amount >= 1 * 1e18
      and balance_amount < 10 * 1e18 then '4. [1, 10) ETH'
      when balance_amount >= 10 * 1e18
      and balance_amount < 20 * 1e18 then '5. [10, 20) ETH'
      when balance_amount >= 20 * 1e18
      and balance_amount < 32 * 1e18 then '6. [20, 32) ETH'
      when balance_amount >= 32 * 1e18
      and balance_amount < 50 * 1e18 then '7. [32, 50) ETH'
      when balance_amount >= 50 * 1e18
      and balance_amount < 100 * 1e18 then '8. [50, 100) ETH'
      when balance_amount >= 100 * 1e18
      and balance_amount < 200 * 1e18 then '9. [100, 200) ETH'
      when balance_amount >= 200 * 1e18
      and balance_amount < 500 * 1e18 then '91. [200, 500) ETH'
      when balance_amount >= 500 * 1e18
      and balance_amount < 1000 * 1e18 then '92. [500, 1000) ETH'
      when balance_amount >= 1000 * 1e18
      and balance_amount < 2000 * 1e18 then '93. [1000, 2000) ETH'
      when balance_amount >= 2000 * 1e18
      and balance_amount < 5000 * 1e18 then '94. [2000, 5000) ETH'
      when balance_amount >= 5000 * 1e18
      and balance_amount < 10000 * 1e18 then '95. [5000, 1W) ETH'
      when balance_amount >= 10000 * 1e18
      and balance_amount < 100000 * 1e18 then '96. [1W, 10W) ETH'
      when balance_amount >= 100000 * 1e18 then '97. [10W, ...) ETH'
    end as Eth_Holding
  from
    eth_balance
  order by
    2 desc
)
select
  Eth_Holding,
  count(address) as Addresses,
  sum(eth_balance_amount) as sum_eth_balance_amount
from
  balance_classification
where
  eth_balance_amount >= 0
group by
  1