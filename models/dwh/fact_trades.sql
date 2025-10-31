{{ config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'trade_id',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['trade_id', 'user_id', 'token_id', 'side_id']
) }}

select
    trade_id,
    user_id,
    token_id,
    if(side = "BUY", "TSD1", "TSD2") as side_id,
    price_usd,
    quantity,
    price_usd * quantity as trade_amount_usd,
    if(status = "FILLED", "TST1", "TST2") as status_id,
    trade_created_time,
    trade_updated_time,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_trades') }} 

{% if is_incremental() %}
-- only process new or updated rows
where trade_updated_time > (select max(trade_updated_time) from {{ this }})
{% endif %}

qualify row_number() over (partition by trade_id order by trade_updated_time desc) = 1