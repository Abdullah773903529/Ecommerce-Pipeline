from config import spark
from pyspark.sql.functions import col, to_timestamp

customers_bronze = spark.read.parquet('ingestion/bronze/customers')
order_items_bronze = spark.read.parquet('ingestion/bronze/order_items')
order_payments_bronze = spark.read.parquet('ingestion/bronze/order_payments')
orders_bronze = spark.read.parquet('ingestion/bronze/orders')
products_bronze = spark.read.parquet('ingestion/bronze/products')
sellers_bronze = spark.read.parquet('ingestion/bronze/sellers')

customers_silver = (
    customers_bronze
    .filter(
        col("customer_id").isNotNull() &
        col("customer_unique_id").isNotNull()
    )
    .dropDuplicates(["customer_id"])
)


order_items_silver = (
    order_items_bronze
    .filter(col('order_id').isNotNull() &
             col('order_item_id').isNotNull() & 
             col('product_id').isNotNull() &
             col('seller_id').isNotNull() &
             col('price').isNotNull())
    .withColumn(
       "shipping_limit_date",
        to_timestamp("shipping_limit_date")
    )
    .filter(col("shipping_limit_date").isNotNull())
    .filter(col("price") > 0)
    .dropDuplicates(['order_id', 'order_item_id'])
           
)


order_payments_silver = (
    order_payments_bronze
    .filter(col('order_id').isNotNull() &
             col('payment_type').isNotNull() &
             col('payment_installments').isNotNull() &
             col('payment_value').isNotNull() &
             (col("payment_value") > 0 )&
             (col("payment_installments") > 0)) 
    .dropDuplicates(['order_id', 'payment_sequential'])
)


orders_silver = (
    orders_bronze.filter(
    col("order_status").isNotNull() &
    col('customer_id').isNotNull() &
    col('order_id').isNotNull())
    .dropDuplicates(['order_id'])
)
dates = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]
for date in dates:
    orders_silver = orders_silver.withColumn(date,to_timestamp(date))

orders_silver = orders_silver.filter(
    col("order_purchase_timestamp").isNotNull()
)


products_silver = (
    products_bronze
    .filter(
        col('product_id').isNotNull() &
        col("product_category_name").isNotNull())
    .dropDuplicates(['product_id'])
    
)

sellers_silver = (
    sellers_bronze
    .filter(
        col('seller_id').isNotNull() &
        col("seller_city").isNotNull() &
        col("seller_state").isNotNull()
)
    .dropDuplicates(['seller_id'])
)



def save_silver_table(df, path, table_name):
    """
    Save DataFrame to Silver layer.
    """
    df.write.mode("overwrite").parquet(path)
    print(f"{table_name} silver table created successfully")


save_silver_table(
    customers_silver,
    "ingestion/silver/customers",
    "customers"
)
save_silver_table(
    order_items_silver,
    "ingestion/silver/order_items",
    "order_items"
)

save_silver_table(
    order_payments_silver,
    "ingestion/silver/order_payments",
    "order_payments"
)

save_silver_table(
    orders_silver,
    "ingestion/silver/orders",
    "orders"
)

save_silver_table(
    products_silver,
    "ingestion/silver/products",
    "products"
)

save_silver_table(
    sellers_silver,
    "ingestion/silver/sellers",
    "sellers"
)

spark.stop()
print("spark stoped successfully")