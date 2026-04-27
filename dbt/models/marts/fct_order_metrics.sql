select
    order_date,
    country,
    count(*) as order_count,
    sum(case when status = 'completed' then quantity else 0 end) as completed_units,
    sum(case when status = 'completed' then gross_revenue else 0 end) as completed_revenue,
    sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    sum(case when status = 'returned' then 1 else 0 end) as returned_orders
from {{ ref('stg_orders') }}
group by 1, 2
