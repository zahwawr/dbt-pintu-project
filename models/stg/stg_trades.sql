{{ config(
    materialized = 'table'
) }}

select
    cast(upper(trade_id) as string) as trade_id,
    cast(upper(user_id) as string) as user_id,
    cast(upper(token_id) as string) as token_id,
    cast(upper(side) as string) as side,
    cast(price_usd as numeric) as price_usd,
    cast(quantity as bignumeric) as quantity,
    cast(upper(status) as string) as status,
    cast(trade_created_time as timestamp) as trade_created_time,
    cast(trade_updated_time as timestamp) as trade_updated_time,
    current_datetime("Asia/Jakarta") as ingested_at
from {{ ref('raw_trades') }}
