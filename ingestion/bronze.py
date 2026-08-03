from config import spark


base_path = '/app/datasets/'

tables = {
    'customers':'olist_customers_dataset.csv',
    'order_items':'olist_order_items_dataset.csv',
    'order_payments':'olist_order_payments_dataset.csv',
    'orders':'olist_orders_dataset.csv',
    'products':'olist_products_dataset.csv',
    'sellers':'olist_sellers_dataset.csv',
}

for table, file in tables.items():
    df = (
        spark.read
        .option('header','true')
        .option('inferSchema','true')
        .csv(base_path+file)
    )
    df.write.mode('overwrite').parquet(f'ingestion/bronze/{table}')

print('Ingestion completed successfully!')

spark.stop()
print('Spark session stopped.')
