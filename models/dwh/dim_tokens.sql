{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['token_id']
) }}

select
    token_id,
    token_name,
    category,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_tokens') }}
