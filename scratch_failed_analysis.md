# Failed Queries Analysis

### hard_05 (hard)
- **Query**: Doanh thu item và số lượng item theo từng quý cho danh mục sản phẩm cama_mesa_banho.
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    dp.product_category_name_english AS category,
    SUM(foi.item_total_value) AS total_revenue,
    COUNT(foi.order_item_key) AS total_products_sold
FROM iceberg.gold.fact_order_item foi
JOIN iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
JOIN iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
WHERE dp.product_category_name_english = 'cama_mesa_banho'
GROUP BY dp.product_category_name_english, d.quarter
ORDER BY d.quarter, total_revenue DESC
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
GROUP BY d.is_weekend
ORDER BY avg_delivery_time ASC
```

### hard_21 (hard)
- **Query**: Các danh mục sản phẩm có tổng doanh thu trong năm 2018 cao hơn năm 2017 (dùng HAVING để lọc).
- **Reason**: row_count_below_min
- **Error**: 
```sql
SELECT 
    dp.product_category_name_english AS category,
    SUM(CASE WHEN d.year = 2018 THEN foi.item_total_value ELSE 0 END) AS revenue_2018,
    SUM(CASE WHEN d.year = 2017 THEN foi.item_total_value ELSE 0 END) AS revenue_2017,
    (SUM(CASE WHEN d.year = 2018 THEN foi.item_total_value ELSE 0 END) - SUM(CASE WHEN d.year = 2017 THEN foi.item_total_value ELSE 0 END))
    / NULLIF(SUM(CASE WHEN d.year = 2017 THEN foi.item_total_value ELSE 0 END), 0) * 100.0 AS growth_rate_pct
FROM 
    iceberg.gold.fact_order_item foi
JOIN 
    iceberg.gold.dim_product dp ON foi.product_key = dp.product_key
JOIN 
    iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
WHERE 
    d.year IN (2017, 2018)
GROUP BY 
    dp.product_category_name_english
HAVING 
    revenue_2018 > revenue_2017
ORDER BY 
    growth_rate_pct DESC
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
    COUNT(DISTINCT foi2.order_item_key) AS total_items_with_cama_mesa_banho
FROM 
    iceberg.gold.fact_order_item foi1
JOIN 
    iceberg.gold.dim_product dp ON foi1.product_key = dp.product_key
JOIN 
    iceberg.gold.fact_order_item foi2 ON foi1.customer_key = foi2.customer_key AND foi1.order_id = foi2.order_id
WHERE 
    dp.product_category_name_english = 'cama_mesa_banho'
GROUP BY 
    dp.product_category_name_english
ORDER BY 
    total_items_with_cama_mesa_banho DESC
LIMIT 3
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
        d.quarter,
        ROW_NUMBER() OVER (PARTITION BY d.quarter ORDER BY total_revenue DESC) AS rank
    FROM iceberg.gold.fact_order_item foi
    JOIN iceberg.gold.dim_seller ds ON foi.seller_key = ds.seller_key
    JOIN iceberg.gold.dim_date d ON foi.purchase_date_key = d.date_key
    WHERE d.year = 2018 AND foi.order_status = 'delivered'
    GROUP BY ds.seller_id, ds.seller_city, ds.seller_state, d.quarter
)
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_revenue,
    quarter
FROM quarterly_revenue
WHERE rank <= 3
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

