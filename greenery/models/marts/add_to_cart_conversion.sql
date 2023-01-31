with
count_session as(
  select
    session_id,
    case
      when event_type = 'add_to_cart' then session_id
    end as session_with_add_to_cart
  from {{ref('stg_greenery__events')}}),
  
final as(
  select
    count(distinct session_with_add_to_cart)::float / count(distinct session_id)::float as conversion_rate
  from count_session
  )

select * from final