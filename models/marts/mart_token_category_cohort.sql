{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['category']
) }}

with
trade as (
    select
        user_id,
        category,
        date_trunc(trade_created_time, month) as month_trx
    from {{ ref('mart_trade_details') }}
    where status = 'FILLED'
    group by all
)
, sender as (
    select
        sender_id as user_id,
        category,
        date_trunc(transfer_created_time, month) as month_trx
    from {{ ref('mart_p2p_transfers_details') }}
    where status = 'SUCCESS'
    group by all
)
, receiver as (
    select
        receiver_id as user_id,
        category,
        date_trunc(transfer_created_time, month) as month_trx
    from {{ ref('mart_p2p_transfers_details') }}
    where status = 'SUCCESS'
    group by all
)
, base_data as (
    select user_id, category, month_trx from trade
    union distinct
    select user_id, category, month_trx from sender
    union distinct
    select user_id, category, month_trx from receiver
)
, first_trx as (
    select
        user_id,
        category,
        min(month_trx) as first_month
    from base_data
    group by all
)
, base_retention as (
    select 
        b.user_id, 
        b.category, 
        b.month_trx,
        f.first_month,
        timestamp_diff(date(b.month_trx), date(f.first_month), month) as diff 
    from base_data b
    inner join first_trx f using (user_id, category)
)
, cohort_index as (
    select 
        category,
        first_month,
        diff,
        count(distinct user_id) as active_users
    from base_retention
    group by all
)
, cohort_max_diff as (
    select 
        category,
        first_month,
        max(diff) as max_diff
    from cohort_index
    group by all
)
, diff_triangle as (
    select
        category,
        first_month,
        diff_val as diff
    from cohort_max_diff, unnest(generate_array(0, max_diff)) as diff_val
)
, recap as (
    select
        c.category,
        c.first_month,
        c.diff,
        c.active_users,
        first_value(c.active_users) over (partition by c.category, c.first_month order by c.diff) AS total_users_first_month
    from cohort_index c
)

select
    t.category,
    date(t.first_month) as first_month,
    t.diff,
    ifnull(r.active_users, 0) as total_customers,
    r_cohort_size.total_users_first_month,
    round(ifnull(r.active_users, 0) / r_cohort_size.total_users_first_month, 2) AS retention_rate,
    current_datetime("Asia/Jakarta") as updated_at
from diff_triangle t
left join recap r using(category, first_month, diff)
inner join recap r_cohort_size on r_cohort_size.category = t.category and r_cohort_size.first_month = t.first_month and r_cohort_size.diff = 0
