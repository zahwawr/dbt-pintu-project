{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['status_id']
) }}

select distinct
    if(status = "SUCCESS", "PTST1", "PTST2") as status_id,
    status,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_p2p_transfers') }}
