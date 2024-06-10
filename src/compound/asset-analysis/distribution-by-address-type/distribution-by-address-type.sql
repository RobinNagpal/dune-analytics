with price as (
select symbol , decimals, avg(token_price_usd) as price 
from dex.prices_latest,tokens.erc20
where token_address = {{token_address}}
and contract_address = {{token_address}}
and blockchain = '{{chain}}'
group by 1,2
),

raw as (
select "from" as address, sum(cast(value as double)*-1) as amount
from 
erc20_{{chain}}.evt_Transfer
where contract_address = {{token_address}}
group by 1
union all 
select "to" as address, sum(cast(value as double)) as amount
from erc20_{{chain}}.evt_Transfer
where contract_address = {{token_address}}
group by 1),

fund_address as (
select address
FROM (
VALUES
(0x820fb25352bb0c5e03e07afc1d86252ffd2f0a18, 'Paradigm'),
(0x0716a17fbaee714f1e6ab0f9d59edbc5f09815c0, 'Jump Trading')
) AS t(address,name)
union all 
select distinct address from labels.funds
)

select 
type, 
sum(amount) as total_holdings from (
select 
address,
case 
when address in (select distinct address from cex_evms.addresses) 
or address in (select distinct address from query_2296923)
then 'CEX'
when address in (select distinct project_contract_address from dex.trades) then 'DEX'
when address in (select distinct address from safe.safes_all) then 'Multi-Sig Wallet'
when address in (select distinct address from {{chain}}.creation_traces)
and address not in (select distinct project_contract_address from dex.trades)
and address not in (select distinct address from safe.safes_all)
and address not in (select distinct address from fund_address) then 'Other Smart Contracts'
when address in (select distinct address from fund_address) then 'VCs/Fund'
else 'Individual Address'
end as type,
sum(amount/power(10,decimals)) as amount,
sum(amount*price/power(10,decimals)) as value
from price, raw 
where address <> 0x0000000000000000000000000000000000000000
group by 1,2)a
where value>1
group by 1

