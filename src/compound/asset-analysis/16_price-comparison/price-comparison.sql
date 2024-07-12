WITH
  token_prices AS (
    SELECT
      DATE(minute) AS day,
      avg(price) AS token_price
    FROM
      prices.usd
    WHERE
      contract_address = {{token_address}}
      and blockchain = '{{chain}}'
      AND minute >= CURRENT_DATE - INTERVAL '3' month
    GROUP BY
      DATE(minute)
  ),
  eth_prices AS (
    SELECT
      DATE(minute) AS day,
      avg(price) AS ETH_price
    FROM
      prices.usd
    WHERE
      symbol = 'ETH'
      AND contract_address IS NULL
      AND minute >= CURRENT_DATE - INTERVAL '3' month
    GROUP BY
      DATE(minute)
  ),
  uni_prices AS (
    SELECT
      DATE(minute) AS day,
      avg(price) AS UNI_price
    FROM
      prices.usd
    WHERE
      contract_address = 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984
      AND minute >= CURRENT_DATE - INTERVAL '3' month
    GROUP BY
      DATE(minute)
  ),
  link_prices AS (
    SELECT
      DATE(minute) AS day,
      avg(price) AS LINK_price
    FROM
      prices.usd
    WHERE
      contract_address = 0x514910771af9ca656af840dff83e8264ecf986ca
      AND minute >= CURRENT_DATE - INTERVAL '3' month
    GROUP BY
      DATE(minute)
  )
SELECT
  l.day,
  l.token_price,
  e.ETH_price,
  u.UNI_price,
  k.LINK_price
FROM
  token_prices l
  INNER JOIN eth_prices e ON l.day = e.day
  INNER JOIN uni_prices u ON l.day = u.day
  INNER JOIN link_prices k ON l.day = k.day
ORDER BY
  l.day;