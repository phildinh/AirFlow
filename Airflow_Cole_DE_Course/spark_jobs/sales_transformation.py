from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("SalesDataTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def transform_sales_data(spark, sales_path, customers_path, products_path, output_path):
    # Read data
    sales_df = spark.read.parquet(sales_path)
    customers_df = spark.read.parquet(customers_path)
    products_df = spark.read.parquet(products_path)
    
    # Join data
    enriched_sales = sales_df \
        .join(customers_df, sales_df.customer_id == customers_df.id, 'left') \
        .join(products_df, sales_df.product_id == products_df.id, 'left')
    
    # Perform aggregations
    daily_summary = enriched_sales.groupBy(
        'segment', 'category'
    ).agg(
        sum('amount').alias('total_sales'),
        count('*').alias('transaction_count'),
        avg('amount').alias('avg_transaction_value'),
        countDistinct('customer_id').alias('unique_customers')
    )
    
    # Add derived metrics
    final_df = daily_summary.withColumn(
        'revenue_per_customer', 
        col('total_sales') / col('unique_customers')
    ).withColumn(
        'processing_date', 
        current_date()
    )
    
    # Write results
    final_df.write \
        .mode('overwrite') \
        .parquet(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-date', required=True)
    parser.add_argument('--sales-path', required=True)
    parser.add_argument('--customers-path', required=True)
    parser.add_argument('--products-path', required=True)
    parser.add_argument('--output-path', required=True)
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    transform_sales_data(
        spark, 
        args.sales_path, 
        args.customers_path, 
        args.products_path, 
        args.output_path
    )
    spark.stop()