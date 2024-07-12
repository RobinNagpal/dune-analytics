with
  data as (
    select
      minute,
      price
    from
      prices.usd
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
  ),
  data_max_price as (
    select
      hour,
      median_price as price
    from
      dex.prices
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
  ),
  min_price_data as (
    select
      min(price) as min_price
    from
      data
  ),
  max_price_data as (
    select
      max(price) as max_price
    from
      data_max_price
  ),
  avg_price_data as (
    select
      avg(price) as avg_price
    from
      data
  ),
  avg_price_24h_data as (
    select
      avg(price) as avg_price_24h
    from
      data
    where
      minute >= CURRENT_TIMESTAMP - INTERVAL '24' hour
  )
select
  (
    select
      min_price
    from
      min_price_data
  ) as min_price,
  (
    select
      max_price
    from
      max_price_data
  ) as max_price,
  (
    select
      avg_price
    from
      avg_price_data
  ) as avg_price,
  (
    select
      avg_price_24h
    from
      avg_price_24h_data
  ) as avg_price_24h;