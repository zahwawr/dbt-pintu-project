with

base as (
  select distinct 
  token_id,
  percentile_cont(amount,0.25) over(partition by token_id) as q1,
  percentile_cont(amount,0.75) over(partition by token_id) as q3
  from {{ ref('mart_p2p_transfers_details') }}
  where status='SUCCESS'
)

, upper_th as (
  select 
  token_id,
  q3 + 1.5 * (q3-q1) as upper_outlier
  from base 
)

select 
  transfer_id,
  transfer_created_time, 
  sender_id,
  receiver_id,
  token_id,
  amount, 
  upper_outlier,
  round(((amount/upper_outlier)-1)*100) pct_diff_to_outlier
from {{ ref('mart_p2p_transfers_details') }}
left join upper_th using (token_id)
where status='SUCCESS'
and amount>upper_outlier