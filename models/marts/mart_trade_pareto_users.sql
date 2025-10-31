{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['month', 'user_id']
) }}

with

base as (
  select date_trunc(date(td.trade_created_time), month) as month, user_id, sum(trade_amount_usd) as total_trade_amount
  from {{ ref('mart_trade_details') }} td
  where status = "FILLED"
  group by all
  order by total_trade_amount desc
)

, pareto_calculation as (
  select *,
    safe_divide(sum(total_trade_amount) over (partition by month order by total_trade_amount desc), sum(total_trade_amount) over (partition by month)) as pctg_pareto,
    row_number() over (partition by month order by total_trade_amount desc) as ranks
  from base
)

, flagging_pareto as (
  select *, if(pctg_pareto < 0.81 or ranks = 1, 1, 0) as is_pareto
  from pareto_calculation
)

select *,
    current_datetime("Asia/Jakarta") as updated_at
from flagging_pareto