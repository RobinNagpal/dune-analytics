# About

The query for total number of holders helps determine how many unique addresses hold the specified token in non-trivial amounts (at least 1 token).
The graph for number of holders over time displays the total number of unique addresses holding an asset with a threshold range and over a period of time.

# Graph

![totalNumberOfHolders](total-number-of-holders.png)
![numberOfHoldersOverTime](number-of-holders-over-time.png)

# Relevance

Understanding Asset Distribution:
By knowing number of holders, we get to know how widely it is being used, more number of holders show asset’s adoption. Low number represents biased and a pre made setup.

Assessing Popularity and Trust:
The number of holders show the asset's popularity. An asset with more number of holders show that it is more trustworthy and in demand.

Market Sentiment:
number of holders over time can provide insights into market activity. For example, a sudden increase in the number of holders indicate positive news or increased investor interest, while a decrease might suggest negative sentiment or sell-offs.

Identifying Trends and Patterns:
Number of holders can help identify trends, such as whether the asset is becoming more popular or if there is a concentration of holdings among a few addresses, which might indicate potential market manipulation or whale activity. Number of holders and how much they are holding and what has been the trend over time to show the stability and to show that it hasnt gained popularity just now.

# Query Explanation

## total number of holders

This query calculates the number of unique holders of a given token on a given blockchain. It does so by:

- Calculating the total amount of tokens received and sent by each address.
- Aggregating these transfers to determine the net holdings for each address.
- Filtering out addresses with net holdings less than 1 token.
- Counting the number of unique addresses with net holdings of at least 1 token.

Token Details retrieves the symbol and decimals of the token with the specified contract address and blockchain

```sql
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
  )
```

Raw CTE aggregates the net transfer amounts for each address by summing the transferred values both "from" and "to" the specified token contract address

```sql
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
```

Finally counts the number of unique addresses.

```sql
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
```

### Tables used

- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

### Alternative Choices

- {{Blockchain}}.transactions (Raw data of a chain containing all kinds of transactions)

## number of holders over time

The query aims to calculate the cumulative number of unique token holders on a daily basis for a specific token on a given blockchain. It does so by tracking the first day each address held a balance, and then sum up the new holders cumulatively.

Daily Holdings CTE calculates the daily balance of tokens received by each address.

- Truncates the event block time to the day level.
- Aggregates the token transfers by day and address, converting the token value to a human-readable format.
- Groups the results by day and address.

```sql
daily_holdings AS (
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        "to" AS address,
        SUM(CAST(value AS DOUBLE) / POW(10, b.decimals)) AS balance
    FROM
        erc20_{{chain}}.evt_Transfer a
        JOIN tokens.erc20 b ON a.contract_address = b.contract_address
    WHERE
        a.contract_address = {{token_address}}
    GROUP BY
        day,
        address
)
```

Daily balance CTE calculates the cumulative daily balance for each address. Sum the balances for each address over time, partitioned by address and ordered by day.

```sql
daily_balances AS (
    SELECT
        day,
        address,
        SUM(balance) OVER (
            PARTITION BY address
            ORDER BY day
        ) AS cumulative_balance
    FROM
        daily_holdings
)
```

Filter balances CTE filters out addresses with non-positive cumulative balances. Selects only the records where the cumulative balance is greater than zero.

```sql
filtered_balances AS (
    SELECT
        day,
        address,
        cumulative_balance
    FROM
        daily_balances
    WHERE
        cumulative_balance > 0
)
```

Distinct holders CTE extracts distinct holders for each day.
Finally counts the number of unique addresses. Selects distinct combinations of day and address from the filtered balances.

```sql
distinct_holders AS (
    SELECT DISTINCT
        day,
        address
    FROM
        filtered_balances
)
```

First Seen CTE finds the first day each address held tokens. Groups by address and finds the minimum day for each address, indicating the first day the address had a positive balance.

```sql
first_seen AS (
    SELECT
        address,
        MIN(day) AS first_seen_day
    FROM
        distinct_holders
    GROUP BY
        address
)
```

Holders per day CTE counts the number of new holders each day. Groups by the first seen day and counts the number of addresses that became holders on that day.

```sql
holders_per_day AS (
    SELECT
        first_seen_day AS day,
        COUNT(*) AS new_holders
    FROM
        first_seen
    GROUP BY
        first_seen_day
)
```

Cumulative holders CTE calculates the cumulative number of holders over time. Sum the number of new holders cumulatively for each day.

```sql
cumulative_holders AS (
    SELECT
        day,
        SUM(new_holders) OVER (
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_number_of_holders
    FROM
        holders_per_day
)
```

Finally outputs the cumulative number of holders for each day ordered by days.

```sql
SELECT
    day,
    cumulative_number_of_holders
FROM
    cumulative_holders
ORDER BY
    day;
```

### Tables used

- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

### Alternative Choices

- {{Blockchain}}.transactions (Raw data of a chain containing all kinds of transactions)
