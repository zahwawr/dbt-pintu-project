{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['region']
) }}

with

trade as (
  select distinct 
    user_id,
    category,
    region,
    date_trunc(trade_created_time, month) as month
  from {{ ref('mart_trade_details') }}
  where status='FILLED'
)

, sender as (
  select distinct 
    sender_id as user_id,
    category,
    sender_region as region,
    date_trunc(transfer_created_time, month) as month
  from {{ ref('mart_p2p_transfers_details') }}
  where status='SUCCESS'
)

, receiver as 
(
  select distinct 
    receiver_id as user_id,
    category,
    receiver_region as region,
    date_trunc(transfer_created_time, month) as month
  from {{ ref('mart_p2p_transfers_details') }}
  where status='SUCCESS'
)

, base_data as 
(
  select distinct 
    user_id,
    region,
    month as month_trx
  from trade 
  full join sender using (user_id,month,region,category)
  full join receiver using (user_id,month,region,category)
)

, first_trx as 
(
  select
    user_id,
    region,
    min(month_trx) as first_month
  from base_data 
  group by all 
)

, base_retention as 
(
  select *,
    timestamp_diff(date(month_trx),date(first_month),month) diff 
  from first_trx 
  inner join base_data using (user_id,region)
)

, cohort_index as (
  select 
    region,
    first_month,
    month_trx,
    diff,
    count(distinct user_id) as active_users
from base_retention
group by all
)

SELECT
  region,
  date(first_month) as first_month,
  diff,
  active_users as total_users,
  FIRST_VALUE(active_users) OVER (PARTITION BY region ORDER BY diff) AS total_users_first_month,
  ROUND(active_users / FIRST_VALUE(active_users) OVER (PARTITION BY region ORDER BY diff), 2) AS retention_rate,
  current_datetime("Asia/Jakarta") as updated_at
FROM cohort_index