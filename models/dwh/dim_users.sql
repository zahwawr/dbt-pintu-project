{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['user_id']
) }}

select
    user_id,
    region,
    signup_date,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_users') }}
