with orders as (

    select * from {{ ref('neworder') }}

),


order_payments as (

    select
        order_trip_id,

        sum(gross_bookings_usd) as total_amount

    from orders

    group by order_trip_id

),

final as (

    select
        orders.order_trip_id,
        orders.customer_id,
        orders.datestr::date as order_date,
        orders.order_status,
        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_trip_id = order_payments.order_trip_id

)

select * from final
