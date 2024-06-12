with
  token_balances as (
    select -- tokens sold
      - sum(cast(value as double) / pow(10, b.decimals)) as amount,
      "from" as address
    from
      erc20_{{chain}}.evt_Transfer a
      join tokens.erc20 b on a.contract_address = b.contract_address
    where
      a.contract_address = {{token_address}}
    group by
      2
    union all
    select -- tokens bought
      sum(cast(value as double) / pow(10, b.decimals)) as amount,
      a.to as address
    from
      erc20_{{chain}}.evt_Transfer a
      join tokens.erc20 b on a.contract_address = b.contract_address
    where
      a.contract_address = {{token_address}}
    group by
      2
  ),
  token_holders as (
    select
      address,
      sum(amount) as balance
    from
      token_balances
    group by
      1
  ),
  token_trades as (
    select
      t.tx_from,
      t.token_bought_address,
      t.token_sold_address,
      t.amount_usd,
      case
        when t.token_bought_address = {{token_address}} then t.token_bought_amount
        else 0
      end as token_bought_amount,
      case
        when t.token_sold_address = {{token_address}} then t.token_sold_amount
        else 0
      end as token_sold_amount,
      t.block_time,
      h.balance
    from
      dex.trades t
      inner join token_holders h on t.tx_from = h.address
    where
      (
        t.token_sold_address = {{token_address}}
        or t.token_bought_address = {{token_address}}
      )
    union all
    select -- adding cowswap agg trades for now; needs to use dex_aggregator in the future
      c.trader as tx_from,
      c.buy_token_address as token_bought_address,
      c.sell_token_address as token_sold_address,
      c.usd_value as amount_usd,
      case
        when c.buy_token_address = {{token_address}} then c.units_bought
        else 0
      end as token_bought_amount,
      case
        when c.sell_token_address = {{token_address}} then c.units_sold
        else 0
      end as token_sold_amount,
      c.block_time,
      h.balance
    from
      cow_protocol_ethereum.trades c
      inner join token_holders h on c.trader = h.address
    where
      (
        c.sell_token_address = {{token_address}}
        or c.buy_token_address = {{token_address}}
      )
  ),
  token_trading_volumes as (
    select
      tx_from,
      max(balance) as balance
    from
      token_trades
    where
      token_bought_address = {{token_address}}
    group by
      1
    union all
    select
      tx_from,
      max(balance) as balance
    from
      token_trades
    where
      token_bought_address <> {{token_address}}
    group by
      1
  ),
  dex_labels_trader_frequencies as (
    select
      address,
      model_name,
      name
    from
      (
        select
          category,
          label_type,
          model_name,
          name,
          blockchain,
          address,
          row_number() over (
            partition by
              category,
              label_type,
              model_name
            order by
              name desc
          ) as rn
        from
          labels.all
        WHERE
          name is not null
          AND address in (
            select
              tx_from
            from
              token_trading_volumes
          )
          and category = 'dex'
          and label_type = 'usage'
          and (model_name = 'trader_frequencies')
      )
  )
select
  name as size,
  count(*) as count_size
from
  dex_labels_trader_frequencies
group by
  name