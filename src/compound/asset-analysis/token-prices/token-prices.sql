with
  data as (
    select
      hour,
      median_price as price
    from
      dex.prices
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
  ),
  latest_data as (
    select
      price as latest_price
    from
      data
    order by
      hour desc
    limit
      1
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
      data
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
      hour >= CURRENT_TIMESTAMP - INTERVAL '24' hour
  )
select
  (
    select
      latest_price
    from
      latest_data
  ) as latest_price,
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