with
  token_details as (
    select
      symbol,
      decimals
    from
      tokens.erc20
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
    group by
      1,
      2
  ),
  raw as (
    select
      "from" as address,
      sum(cast(value as double) * -1) as amount
    from
      erc20_{{chain}}.evt_Transfer
    where
      contract_address = {{token_address}}
    group by
      1
    union all
    select
      "to" as address,
      sum(cast(value as double)) as amount
    from
      erc20_{{chain}}.evt_Transfer
    where
      contract_address = {{token_address}}
    group by
      1
  )
select
  count(distinct address) as holders
from
  (
    select
      address,
      sum(amount / power(10, decimals)) as value
    from
      raw,
      token_details
    group by
      1
  ) a
where
  value > 0