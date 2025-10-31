{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['token_id']
) }}

with base as 
(
  select 
  token_id 
  , round(sum(trade_amount_usd)) volume 
  , count(distinct trade_id) trade 
  from {{ ref('mart_trade_details') }}
  where status='FILLED'
  group by all
) 

select token_id 
, ifnull(trade,0) trade 
, ifnull(trade/sum(trade) over(),0) pct_trade 
, ifnull(volume,0) volume
, ifnull(volume/sum(volume) over(),0) pct_volume  
, current_datetime("Asia/Jakarta") as updated_at
from {{ ref('dim_tokens')}}
left join base using (token_id)