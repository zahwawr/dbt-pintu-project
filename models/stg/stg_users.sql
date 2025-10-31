{{ config(
    materialized = 'table'
) }}

select
    cast(upper(user_id) as string) as user_id,
    cast(initcap(trim(region)) as string) as region,
    cast(signup_date as date) as signup_date,
    current_datetime("Asia/Jakarta") as ingested_at
from {{ ref('raw_users') }}
