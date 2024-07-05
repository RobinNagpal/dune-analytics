# About

The query for total number of holders helps determine how many unique addresses hold the specified token in non-trivial amounts (at least 1 token).
The graph for number of holders over time displays the total number of unique addresses holding an asset with a threshold range and over a period of time.

# Graph

![totalNumberOfHolders](total-number-of-holders.png)
![numberOfHoldersOverTime](number-of-holders-over-time.png)
![numberOfHoldersOverTimeWithGreaterTokenValueThan$100](number-of-holders-with-greater-than-$100-token-value.png)

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

Transfers CTE aggregates transfer amounts into and out of the token contract address on a daily basis

```sql
transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          CAST(value AS DECIMAL (38, 0)) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          (-1) * (CAST(value AS DECIMAL (38, 0))) AS amount
        FROM
          erc20_{{chain}}.evt_Transfer AS tr
        WHERE
          contract_address = {{token_address}}
      ) AS t
    GROUP BY
      1,
      2,
      3
  )
```

Computes the cumulative token balances for each address over time

```sql
balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      SUM(amount) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS balance,
      LEAD(DAY, 1, CURRENT_TIMESTAMP) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS next_day
    FROM
      transfers AS t
  )
```

Generates a sequence of days from the current date 

```sql
days AS (
    SELECT
      DAY
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST(
            DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP
          ),
          CAST(
            TRY_CAST(
              TRY_CAST(
                TRY_CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP) AS TIMESTAMP
              ) AS TIMESTAMP
            ) AS TIMESTAMP
          ),
          INTERVAL '1' day
        )
      ) AS _u (DAY)
  )
```

Calculates the cumulative token balances for all days up to the present

```sql
balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / TRY_CAST(POWER(10, 0) AS DOUBLE)) AS balance
    FROM
      balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  )
```

Counts the number of unique addresses (address) holding a positive token balance

```sql
SELECT
  COUNT(address) AS "Holders"
FROM
  balance_all_days AS b
WHERE
  balance > 0
```

### Tables used

- tokens.erc20 (Curated dataset for erc20 tokens with addresses, symbols and decimals. Origin unknown)
- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

### Alternative Choices

- {{Blockchain}}.transactions (Raw data of a chain containing all kinds of transactions)

## number of holders over time

The query aims to calculate the cumulative number of unique token holders on a daily basis for a specific token on a given blockchain. It does so by tracking the first day each address held a balance, and then sum up the new holders cumulatively.

- Truncates the event block time to the day level.
- Aggregates the token transfers by day and address, converting the token value to a human-readable format.
- Groups the results by day and address.

This query also uses the same CTE transfers, balance_with_gap_days, days and balance_all_days

The token_holders_with_balance CTE calculates the number of unique addresses (holders) with a positive balance for each day from the token_balance_all_days data

```sql
token_holders_with_balance AS (
    SELECT
      b.day AS "Date",
      COUNT(address) AS "Holders with Balance"
    FROM
      token_balance_all_days AS b
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
```

It finally retrieves the number of holders with a balance greater than zero for three tokens (given token, UNI, and LINK) on each day

```sql
SELECT
  COALESCE(hwb_token."Date", hwb_uni."Date", hwb_link."Date") AS "Date",
  COALESCE(hwb_token."Holders with Balance", 0) AS "Token Holders with Balance",
  COALESCE(hwb_uni."Holders with Balance", 0) AS "UNI Holders with Balance",
  COALESCE(hwb_link."Holders with Balance", 0) AS "LINK Holders with Balance"
FROM
  token_holders_with_balance hwb_token
  FULL JOIN uni_holders_with_balance hwb_uni ON hwb_token."Date" = hwb_uni."Date"
  FULL JOIN link_holders_with_balance hwb_link ON hwb_token."Date" = hwb_link."Date"
ORDER BY
  COALESCE(hwb_token."Date", hwb_uni."Date", hwb_link."Date");
```

### Tables used

- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)

### Alternative Choices

- {{Blockchain}}.transactions (Raw data of a chain containing all kinds of transactions)

## number of holders over time having token value greater than $100

This query calculates the number of Ethereum token holders over time, specifically for a given token, UNI, and LINK. The query also determines the number of holders who possess more than $100 worth of tokens based on daily prices

This query also uses the same CTE transfers, balance_with_gap_days, days and balance_all_days

Calculates the daily average price for each token

```sql
token_daily_prices AS (
    SELECT
      er.decimals,
      DATE_TRUNC('day', hour) AS day,
      AVG(dx.median_price) AS price
    FROM
      dex.prices dx
      JOIN tokens.erc20 er ON er.contract_address = {{token_address}}
    WHERE
      dx.contract_address = {{token_address}}
      AND er.blockchain = '{{chain}}'
    GROUP BY
      er.decimals,
      DATE_TRUNC('day', hour)
  )
```

Counts the number of holders for each day whose token holdings exceed $100

```sql
token_holders_with_token_value AS (
    SELECT
      b.day AS "Date",
      COUNT(
        CASE
          WHEN (balance * p.price / POWER(10, p.decimals)) > 100 THEN address
        END
      ) AS "Holders with Token Value > $100"
    FROM
      token_balance_all_days AS b
      LEFT JOIN token_daily_prices AS p ON b.day = p.day
    WHERE
      balance > 0
    GROUP BY
      b.day
  )
```

The final SELECT statement combines the data from the above CTEs using FULL JOINs to ensure that the result set includes all relevant dates, even if some tokens don't have data for every day

```sql
SELECT
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date") AS "Date",
  COALESCE(htv_token."Holders with Token Value > $100", 0) AS "Token Holders with Token Value > $100",
  COALESCE(htv_uni."Holders with Token Value > $100", 0) AS "UNI Holders with Token Value > $100",
  COALESCE(htv_link."Holders with Token Value > $100", 0) AS "LINK Holders with Token Value > $100"
FROM
  token_holders_with_token_value htv_token
  FULL JOIN uni_holders_with_token_value htv_uni ON htv_token."Date" = htv_uni."Date"
  FULL JOIN link_holders_with_token_value htv_link ON htv_token."Date" = htv_link."Date"
ORDER BY
  COALESCE(htv_token."Date", htv_uni."Date", htv_link."Date");
```

### Tables used

- erc20\_{{Blockchain}}.evt_Transfer (Curated dataset of erc20 tokens' transactions. Origin unknown)
- dex.prices (This table loads the prices of tokens from the dex.trades table. This helps for missing tokens from the prices.usd table. Made by @henrystats. Present in the spellbook of dune analytics [Spellbook-Dex-Prices](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_schema.yml))

### Alternative Choices

- {{Blockchain}}.transactions (Raw data of a chain containing all kinds of transactions)