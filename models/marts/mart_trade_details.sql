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
    tr.trade_id,
    tr.user_id,
    u.region,
    tr.token_id,
    t.token_name,
    t.category,
    tr.side_id,
    sd.side,
    tr.status_id,
    st.status,
    tr.price_usd,
    tr.quantity,
    tr.trade_amount_usd,
    tr.trade_created_time,
    tr.trade_updated_time,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('fact_trades') }} as tr
left join {{ ref('dim_users') }} as u using (user_id)
left join {{ ref('dim_trade_sides') }} as sd using (side_id)
left join {{ ref('dim_trade_status') }} as st using (status_id)
left join {{ ref('dim_tokens') }} as t using (token_id)

{% if is_incremental() %}
-- only process new or updated rows
where trade_updated_time > (select max(trade_updated_time) from {{ this }})
{% endif %}

