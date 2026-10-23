{{ config(tags=['mart']) }}

select
    c.customer_id,
    c.first_name,
    coalesce(sum(o.amount), 0) as total_amount,
    count(e.id) as event_count
from {{ ref('stg_customers') }} as c
left join {{ ref('stg_orders') }} as o on o.customer_id = c.customer_id
left join {{ ref('stg_events') }} as e on e.customer_id = c.customer_id
group by 1, 2
