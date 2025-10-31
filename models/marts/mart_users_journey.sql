{{ config(
    materialized = 'table',
    partition_by={
        "field": "date(updated_at)",
        "data_type": "date"
    },
    cluster_by = ['user_id']
) }}

with

base_trader as (
  select 
    td.user_id,
    du.signup_date,
    min(trade_created_time) as min_trade_time,
    max(trade_created_time) as max_trade_time
  from {{ ref('mart_trade_details') }} td
  left join pintu_dwh.dim_users du using (user_id)
  where status='FILLED'
  group by all 
)

, base_sender as (
  select 
    sender_id as user_id,
    min(transfer_created_time) as min_sender_time,
    max(transfer_created_time) as max_sender_time
  from {{ ref('mart_p2p_transfers_details') }}
  where status='SUCCESS'
  group by all 
)

, base_receiver as (
  select 
    receiver_id as user_id,
    min(transfer_created_time) as min_receiver_time,
    max(transfer_created_time) as max_receiver_time
  from {{ ref('mart_p2p_transfers_details') }}
  where status='SUCCESS'
  group by all 
)

select 
  user_id,
  signup_date,
  case 
    when 
      (min_trade_time<min_receiver_time or min_receiver_time is null)
      and (min_trade_time<min_sender_time or min_sender_time is null)
    then 'Trade'
  else 'P2P'
  end start_from,
  case 
    when 
      (min_sender_time<min_receiver_time or min_receiver_time is null)
      and (min_sender_time<min_trade_time or min_trade_time is null)
    then 'Sender'
    when 
      (min_receiver_time<min_sender_time or min_sender_time is null)
      and (min_receiver_time<min_trade_time or min_trade_time is null)
    then 'Receiver'
  else 'Trader'
  end start_as,
  min_sender_time,
  max_sender_time,
  min_receiver_time,
  max_receiver_time,
  min_trade_time,
  max_trade_time,
  if(min_trade_time is not null,TRUE,FALSE) is_trade,
  if(coalesce(min_sender_time,min_receiver_time) is not null,TRUE,FALSE) is_p2p,
  current_datetime("Asia/Jakarta") as updated_at
from base_trader
full join base_sender using (user_id)
full join base_receiver using (user_id)