/*
- Note down the list of asset addresses. Top 10 asset addresses
- For each of the assets we will run this query to know how many borrowers hold how much each of the asset
- So we write a separate query for each of the assets
- Also display the price of the range of the asset.  So it should have two columns for the range of the asset

*/
with wallets as (
  select
        a.address as wallet_address,
        sum( a.value ) as wallet_balance

  from bitcoin.outputs a
  where not exists (    select 1 from bitcoin.inputs b
                        where b.spent_tx_id = a.tx_id
                    )
    and a.address is not null
    and a.type != 'nulldata'    -- unclassified type of wallet or something
  group by 1
  having sum( a.value ) >= 1
 ),

balance_classification as (
select
        wallet_address,
        wallet_balance,
        case
            when wallet_balance >= 1 and wallet_balance <= 10 then '1 to 10' --know that the data for 1 to 9 wiill be skewed because you're limiting
            when wallet_balance >= 10 and wallet_balance <= 20 then '10 to 20'
            when wallet_balance >= 20 and wallet_balance <= 30 then '20 to 30'
            when wallet_balance >= 30 and wallet_balance <= 40 then '30 to 40'
            when wallet_balance >= 40 and wallet_balance <= 50 then '40 to 50'
            when wallet_balance >= 50 and wallet_balance <= 60 then '50 to 60'
            when wallet_balance >= 60 and wallet_balance <= 70 then '60 to 70'
            when wallet_balance >= 70 and wallet_balance <= 80 then '70 to 80'
            when wallet_balance >= 80 and wallet_balance <= 90 then '80 to 90'
            when wallet_balance >= 90 and wallet_balance <= 100 then '90 to 100'
            end as Btc_Holding

from wallets
order by 2 desc
limit 1000000
)

    select
            Btc_Holding,
            count( wallet_address ) as Addresses

    from balance_classification
    where Btc_Holding is not null
    group by 1


