from config import spark
from pyspark.sql.functions import col, to_timestamp

customers_bronze = spark.read.parquet('ingestion/bronze/customers')
order_items_bronze = spark.read.parquet('ingestion/bronze/order_items')
order_payments_bronze = spark.read.parquet('ingestion/bronze/order_payments')
orders_bronze = spark.read.parquet('ingestion/bronze/orders')
products_bronze = spark.read.parquet('ingestion/bronze/products')
sellers_bronze = spark.read.parquet('ingestion/bronze/sellers')

customers_silver = (
    customers_bronze.dropDuplicates(['customer_id'])
    .filter(col('customer_unique_id').isNotNull())
)
customers_silver.write.mode('overwrite').parquet('ingestion/silver/customers')
print('customers silver table created successfully')

order_items_silver = (
    order_items_bronze.dropDuplicates(['order_id', 'order_item_id'])
    .filter(col('order_id').isNotNull() &
             col('order_item_id').isNotNull() & 
             col('product_id').isNotNull() &
             col('seller_id').isNotNull() &
             col('shipping_limit_date').isNotNull() &
             col('price').isNotNull())
    .withColumn('shipping_limit_date', to_timestamp(col('shipping_limit_date')))
         
)

order_items_silver.write.mode('overwrite').parquet('ingestion/silver/order_items')
print('order_items silver table created successfully')

order_payments_silver = (
    order_payments_bronze.dropDuplicates(['order_id', 'payment_type'])
    .filter(col('order_id').isNotNull() &
             col('payment_type').isNotNull() &
             col('payment_installments').isNotNull() &
             col('payment_value').isNotNull())
)

order_payments_silver.write.mode('overwrite').parquet('ingestion/silver/order_payments')
print('order_payments silver table created successfully')


spark.stop()
print("spark stoped successfully")