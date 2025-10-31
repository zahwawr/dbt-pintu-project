{{ config(
    materialized = 'table'
) }}

select
    cast(upper(transfer_id) as string) as transfer_id,
    cast(upper(sender_id) as string) as sender_id,
    cast(upper(receiver_id) as string) as receiver_id,
    cast(upper(token_id) as string) as token_id,
    cast(amount as numeric) as amount,
    cast(upper(trim(status)) as string) as status,
    cast(transfer_created_time as timestamp) as transfer_created_time,
    cast(transfer_updated_time as timestamp) as transfer_updated_time,
    current_datetime("Asia/Jakarta") as ingested_at
from {{ ref('raw_p2p_transfers') }}
