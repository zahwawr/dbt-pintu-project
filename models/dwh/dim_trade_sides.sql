{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['side_id']
) }}

select distinct
    if(side = "BUY", "TSD1", "TSD2") as side_id,
    side,
    current_datetime("Asia/Jakarta") as updated_at
from {{ ref('stg_trades') }}
