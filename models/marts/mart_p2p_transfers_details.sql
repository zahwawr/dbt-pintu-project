{{ config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'transfer_id',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['transfer_id', 'sender_id', 'receiver_id', 'token_id']
) }}

select
  pt.transfer_id,
  pt.sender_id,
  u1.region as sender_region,
  pt.receiver_id,
  u2.region as receiver_region,
  pt.token_id,
  t.token_name,
  t.category,
  pt.amount,
  pt.status_id,
  ts.status,
  pt.transfer_created_time,
  pt.transfer_updated_time,
  CURRENT_DATETIME("Asia/Jakarta") as updated_at
from {{ ref('fact_p2p_transfers') }} as pt
left join {{ ref('dim_users') }} as u1 on u1.user_id = pt.sender_id
left join {{ ref('dim_users') }} as u2 on u2.user_id = pt.receiver_id
left join {{ ref('dim_tokens') }} as t using (token_id)
left join {{ ref('dim_p2p_transfers_status') }} as ts using (status_id)

{% if is_incremental() %}
-- only process new or updated rows
where transfer_updated_time > (select max(transfer_updated_time) from {{ this }})
{% endif %}
