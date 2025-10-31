{{ config(
    materialized = 'table'
) }}

select
    cast(upper(token_id) as string) as token_id,
    cast(initcap(trim(token_name)) as string) as token_name,
    cast(initcap(trim(category)) as string) as category,
    current_datetime("Asia/Jakarta") as ingested_at
from {{ ref('raw_tokens') }}
