with 
count_order_per_user as(
  select 
    user_id,
    count(*) as count_repeat
  from {{ref('stg_greenery__orders')}}
  group by user_id),

user_bucket as(
  select 
    user_id,
    (count_repeat >= 2)::int as has_two_orders
  from count_order_per_user
),

final as (
    select
        sum(has_two_orders)::float / count(distinct user_id)::float as repeat_rate
    from user_bucket
)
  
select * from final