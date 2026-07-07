# Failed Queries Analysis

### hard_05 (hard)
- **Query**: Doanh thu item và số lượng item theo từng quý cho danh mục sản phẩm cama_mesa_banho.
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT
    dp.product_category_name_english AS category,
    SUM(foi.item_total_value) AS total_revenue,
    COUNT(foi.order_item_key) AS total_items
FROM iceberg.gold.fact_order_item foi
JOIN iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
JOIN iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
WHERE dp.product_category_name_english = 'cama_mesa_banho'
  AND d.quarter IN (1, 2, 3) -- Quarters are 1-4 in Trino
GROUP BY dp.product_category_name_english, d.quarter
ORDER BY d.quarter, total_revenue DESC
```

### hard_13 (hard)
- **Query**: Liệt kê các khách hàng đặt từ 2 đơn hàng trở lên, kèm tổng giá trị thanh toán của họ.
- **Reason**: row_count_below_min
- **Error**: 
```sql
WITH customer_order_counts AS (
    SELECT
        dc.customer_key,
        COUNT(fo.order_key) AS total_orders_per_customer
    FROM iceberg.gold.fact_order fo
    JOIN iceberg.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_key
)
SELECT 
    dc.customer_id,
    SUM(fo.total_payment_value) AS total_revenue
FROM customer_order_counts co
JOIN iceberg.gold.fact_order fo ON co.customer_key = fo.customer_key
JOIN iceberg.gold.dim_customer dc ON fo.customer_key = dc.customer_key
WHERE co.total_orders_per_customer >= 2
GROUP BY dc.customer_id, dc.customer_id -- Corrected to use the correct column name
ORDER BY total_revenue DESC
```

### hard_14 (hard)
- **Query**: So sánh thời gian giao hàng trung bình giữa đơn hàng cuối tuần và ngày thường theo từng tiểu bang khách hàng (Lưu ý: KHÔNG group by bằng alias, hãy copy nguyên cụm CASE hoặc dùng số thứ tự).
- **Reason**: missing_expected_table
- **Error**: 
```sql
SELECT 
    CASE WHEN d.is_weekend = TRUE THEN 'Cuối tuần' ELSE 'Ngày thường' END AS day_type,
    AVG(fo.delivery_actual_days) AS avg_delivery_time
FROM iceberg.gold.fact_order fo
JOIN iceberg.gold.dim_date d ON fo.purchase_date_key = d.date_key
GROUP BY 1
ORDER BY avg_delivery_time DESC
```

### hard_21 (hard)
- **Query**: Các danh mục sản phẩm có tổng doanh thu trong năm 2018 cao hơn năm 2017 (dùng HAVING để lọc).
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    dp.product_category_name_english AS category,
    SUM(foi.item_total_value) AS total_revenue_2018,
    SUM(CASE WHEN d.year = 2017 THEN foi.item_total_value ELSE 0 END) AS total_revenue_2017
FROM 
    iceberg.gold.fact_order_item foi
JOIN 
    iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
JOIN 
    iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
WHERE 
    d.year IN (2018, 2017)
GROUP BY 
    dp.product_category_name_english
HAVING 
    total_revenue_2018 > total_revenue_2017
ORDER BY 
    total_revenue_2018 DESC
```

### hard_23 (hard)
- **Query**: Thời gian giao hàng trung bình của từng tiểu bang seller, phân loại theo: nhanh (<7 ngày), trung bình (7-14 ngày), chậm (>14 ngày).
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    ds.seller_state,
    CASE 
        WHEN AVG(foi.shipping_days) < 7 THEN 'Nhanh'
        WHEN AVG(foi.shipping_days) BETWEEN 7 AND 14 THEN 'Trung bình'
        ELSE 'Chậm'
    END AS shipping_classification,
    ROUND(AVG(foi.shipping_days), 2) AS avg_shipping_days
FROM 
    iceberg.gold.fact_order_item foi
JOIN 
    iceberg.gold.dim_seller ds ON foi.seller_key = ds.seller_key
WHERE 
    foi.order_status = 'delivered'
GROUP BY 
    ds.seller_state, shipping_classification
ORDER BY 
    avg_shipping_days ASC
```

### hard_24 (hard)
- **Query**: Danh mục sản phẩm nào thường được mua kèm nhiều nhất với sản phẩm trong danh mục cama_mesa_banho? (Gợi ý: Tìm các order_key có chứa cama_mesa_banho, sau đó đếm các sản phẩm KHÁC nằm trong cùng order_key đó).
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    dp.product_category_name_english AS category,
    COUNT(foi.order_item_key) AS total_items_sold
FROM iceberg.gold.fact_order_item foi
JOIN iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
WHERE dp.product_category_name_english = 'cama_mesa_banho'
GROUP BY dp.product_category_name_english
ORDER BY total_items_sold DESC
LIMIT 1
```

### hard_25 (hard)
- **Query**: Top 3 seller có doanh thu cao nhất trong mỗi quý của năm 2018 (dùng hàm ROW_NUMBER() OVER (PARTITION BY quarter ORDER BY doanh_thu DESC) để lấy top 3).
- **Reason**: row_count_below_min
- **Error**: 
```sql
WITH quarterly_revenue AS (
    SELECT
        ds.seller_id,
        ds.seller_city,
        ds.seller_state,
        SUM(foi.item_total_value) AS total_revenue,
        DATE_TRUNC('quarter', d.date_key) AS quarter_start_date
    FROM iceberg.gold.fact_order_item foi
    JOIN iceberg.gold.dim_seller ds ON foi.seller_key = ds.seller_key
    JOIN iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
    WHERE d.year = 2018 AND foi.order_status = 'delivered'
    GROUP BY ds.seller_id, ds.seller_city, ds.seller_state, quarter_start_date
)
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_revenue,
    quarter_start_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY quarter_start_date ORDER BY total_revenue DESC) AS rn
    FROM quarterly_revenue
) ranked_sellers
WHERE rn <= 3
```

### hard_27 (hard)
- **Query**: Phân khúc khách hàng theo tổng chi tiêu: dưới 100, 100-500, trên 500, kèm số lượng và tổng doanh thu mỗi phân khúc.
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    CASE 
        WHEN fo.total_payment_value < 100 THEN 'Under 100'
        WHEN fo.total_payment_value BETWEEN 100 AND 500 THEN '100-500'
        ELSE 'Above 500'
    END AS customer_segment,
    COUNT(fo.order_key) AS order_count,
    SUM(fo.total_payment_value) AS total_revenue
FROM iceberg.gold.fact_order fo
GROUP BY customer_segment
ORDER BY customer_segment
```

### hard_28 (hard)
- **Query**: Top 5 cặp (thành phố khách hàng - danh mục sản phẩm) có số lượng item bán ra nhiều nhất (bắt buộc join qua bảng fact_order để kết nối customer và order_item).
- **Reason**: missing_expected_table
- **Error**: 
```sql
SELECT 
    dc.customer_city AS city,
    dp.product_category_name_english AS category,
    COUNT(foi.order_item_key) AS total_items_sold
FROM 
    iceberg.gold.fact_order fo
JOIN 
    iceberg.gold.dim_customer dc ON fo.customer_key = dc.customer_key
JOIN 
    iceberg.gold.fact_order_item foi ON fo.order_id = foi.order_id
JOIN 
    iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
GROUP BY 
    dc.customer_city, dp.product_category_name_english
ORDER BY 
    total_items_sold DESC
LIMIT 5
```

### hard_30 (hard)
- **Query**: Tỷ lệ khách hàng mua hàng lặp lại (mua từ 2 đơn trở lên) theo từng tiểu bang.
- **Reason**: missing_expected_table
- **Error**: 
```sql
WITH customer_order_counts AS (
    SELECT
        dc.customer_state,
        dc.customer_unique_id,
        COUNT(fo.order_key) AS total_orders_per_customer
    FROM iceberg.gold.fact_order fo
    JOIN iceberg.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_state, dc.customer_unique_id
)
SELECT
    customer_state,
    COUNT(customer_unique_id) AS total_customers,
    SUM(CASE WHEN total_orders_per_customer >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(SUM(CASE WHEN total_orders_per_customer >= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(customer_unique_id), 2) AS repeat_rate_pct
FROM customer_order_counts
GROUP BY customer_state
ORDER BY repeat_rate_pct DESC
```

