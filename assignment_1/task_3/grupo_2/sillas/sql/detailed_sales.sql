SELECT
    dim_dates.full_date,
    dim_products.product_line,
    dim_products.product_name,
    dim_countries.country,
    SUM(fact_orders.sales_amount) AS total_sales
FROM fact_orders
JOIN dim_products
    ON fact_orders.product_id = dim_products.product_id
JOIN dim_countries
    ON fact_orders.country_key = dim_countries.country_key
JOIN dim_dates
    ON fact_orders.order_date_key = dim_dates.date_key
GROUP BY
    dim_dates.full_date,
    dim_products.product_line,
    dim_products.product_name,
    dim_countries.country
