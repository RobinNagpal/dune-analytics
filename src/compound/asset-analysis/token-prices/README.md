# About

Here we show various price metrics for the given token on the specified blockchain. The metrics include the current price, minimum price, maximum price, average price, and average price over the last 24 hours.

# Graph

![tokenPrices](token-prices.png)

# Relevance

Calculating these price metrics is essential for many reasons:

- Current Price: Knowing the latest price of the token helps in making real-time trading decisions and assessing the token's current market value.
- Minimum and Maximum Prices: Understanding the price range (min and max prices) gives insight into the volatility and stability of the token's price over time.
- Average Price: The average price provides a general idea of the token's overall price level and can be used as a benchmark for comparison with the current price.
- Average Price over 24 Hours: The 24-hour average price helps in assessing short-term price trends and market sentiment, which is crucial for traders and investors.

# Query Explanation

The query simply selects the median prices from the DEX prices table for the given token and blockchain. Then calculates the price metrics by using aggregate functions and time interval.

Data CTE selects the `median_price` and `hour` from the `dex.prices` table where the token address and blockchain match the given values.

```sql
data as (
    select
      hour,
      median_price as price
    from
      dex.prices
    where
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
  )
```

Latest data CTE retrieves the latest price by ordering the data by hour in descending order and limiting the result to the most recent entry.

```sql
latest_data as (
    select
      price as latest_price
    from
      data
    order by
      hour desc
    limit
      1
  )
```

Minimum price data CTE calculates the minimum price from the data.

```sql
min_price_data as (
    select
      min(price) as min_price
    from
      data
  )
```

Maximum price data CTE calculates the maximum price from the data.

```sql
max_price_data as (
    select
      max(price) as max_price
    from
      data
  )
```

Average price data CTE calculates the average price from the data.

```sql
avg_price_data as (
    select
      avg(price) as avg_price
    from
      data
  )
```

Average price 24h data CTE calculates the average price over the last 24 hours by filtering the data to include only the rows where hour is within the last 24 hours.

```sql
avg_price_24h_data as (
    select
      avg(price) as avg_price_24h
    from
      data
    where
      hour >= CURRENT_TIMESTAMP - INTERVAL '24' hour
  )
```

Finally retrieves the calculated metrics from each CTE and presents them in a single result set:

```sql
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
```

**Hardcoded addresses**

## Tables used

- dex.prices (This table loads the prices of tokens from the dex.trades table. This helps for missing tokens from the prices.usd table. Made by @henrystats. Present in the spellbook of dune analytics [Spellbook-Dex-Prices](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_schema.yml))

## Alternative Choices
