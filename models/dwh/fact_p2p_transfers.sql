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
    transfer_id,
    sender_id,
    receiver_id,
    token_id,
    amount,
    if(status = "SUCCESS", "PTST1", "PTST2") as status_id,
    transfer_created_time,
    transfer_updated_time,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_p2p_transfers') }}

{% if is_incremental() %}
-- only process new or updated rows
where transfer_updated_time > (select max(transfer_updated_time) from {{ this }})
{% endif %}