with customers as (

    select * from {{ ref('newcustomer') }}

),


orders as (

    select * from {{ ref('neworder') }}

),

customer_orders as (

    select
        customer_id,
        min(datestr) as first_order,
        max(datestr) as most_recent_order,
        count(order_trip_id) as number_of_orders
    from orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(orders.gross_bookings_usd) as total_amount

    from orders

    group by orders.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.signup_date::date,
        customers.tier,
        customers.city,
        customers.state,
        customers.country,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

    left join customer_payments
        on customers.customer_id = customer_payments.customer_id

)

select * from final

