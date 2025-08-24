-- @label: create_final_table
CREATE TABLE IF NOT EXISTS final_processed_sales (
    transaction_id STRING,
    sale_amount DOUBLE,
    product_name STRING,
    category STRING
)
PARTITIONED BY (sale_date DATE)
STORED AS PARQUET;


-- @label: create_temp_table
CREATE TEMPORARY TABLE temp_filtered_sales AS
SELECT
    transaction_id,
    product_id,
    sale_amount
FROM sales_data
WHERE
    region = 'North America' AND sale_date = '{date_str}';


-- @label: insert_output_table
INSERT OVERWRITE TABLE final_processed_sales PARTITION(sale_date='{date_str}')
SELECT
    t1.transaction_id,
    t1.sale_amount,
    t2.product_name,
    t2.category
FROM temp_filtered_sales t1
JOIN product_info t2 ON t1.product_id = t2.product_id;