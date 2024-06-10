with 
price as (
select symbol , decimals, avg(token_price_usd) as price 
from dex.prices_latest,tokens.erc20
where token_address = {{Token Contract Address}}
and contract_address = {{Token Contract Address}}
and blockchain = '{{Blockchain}}'
group by 1,2
),

raw as (
select "from" as address, sum(cast(value as double)*-1) as amount
from 
erc20_{{Blockchain}}.evt_Transfer
where contract_address = {{Token Contract Address}}
group by 1
union all 
select "to" as address, sum(cast(value as double)) as amount
from erc20_{{Blockchain}}.evt_Transfer
where contract_address = {{Token Contract Address}}
group by 1)

select 
case 
when percent_holdings >=0.5 then 'H) Holdings >=50%'
when percent_holdings >=0.4 and percent_holdings<0.5 then 'G) Holdings >=40% & <50%'
when percent_holdings >=0.3 and percent_holdings<0.4 then 'F) Holdings >=30% & <40%'
when percent_holdings >=0.2 and percent_holdings<0.3 then 'E) Holdings >=20% & <30%'
when percent_holdings >=0.1 and percent_holdings<0.2 then 'D) Holdings >=10% & <20%'
when percent_holdings >=0.05 and percent_holdings<0.1 then 'C) Holdings >=5% & <10%'
when percent_holdings >=0.01 and percent_holdings<0.05 then 'B) Holdings >=1% & <5%'
when percent_holdings <0.01 then 'A) Holdings <1%'
end as distribution,
count(distinct address) as address_count,
sum(holding) as total_holding
from (
select 
address,
sum(amount/power(10,decimals)) as holding,
sum(amount*price/power(10,decimals)) as holding_usd,
sum(amount)/(select sum(amount) from raw where address not in (0x0000000000000000000000000000000000000000,0x000000000000000000000000000000000000dEaD,0xD15a672319Cf0352560eE76d9e89eAB0889046D3)) as percent_holdings
from price, raw 
where address not in (0x0000000000000000000000000000000000000000,0x000000000000000000000000000000000000dEaD,0xD15a672319Cf0352560eE76d9e89eAB0889046D3)
group by 1
)a
where holding_usd>1
group by 1


