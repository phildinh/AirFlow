
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import os
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@company.com']
}

# Create DAG
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='Daily sales data processing pipeline with Spark',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    max_active_runs=1,
    catchup=False,
    tags=['sales', 'etl', 'spark']
)

def extract_sales_data(**context):
    """Extract sales data from PostgreSQL"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Connect to sales database
        pg_hook = PostgresHook(postgres_conn_id='sales_db_conn')
        
        sql = f"""
            SELECT 
                id,
                customer_id,
                product_id,
                amount,
                quantity,
                created_at,
                updated_at
            FROM sales 
            WHERE DATE(created_at) = '{date_str}' OR '{date_str}' = '2024-01-01'
        """
        
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} sales records for {date_str}")
        
        # Save to local file system (simulating S3)
        output_dir = f"/opt/data/raw/sales/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/sales.parquet"
        df.to_parquet(output_path)
        
        logging.info(f"Saved sales data to {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error extracting sales data: {e}")
        raise

def extract_customer_data(**context):
    """Extract customer data from REST API"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Call mock customer API
        response = requests.get(
            'http://customer_api:5000/customers',
            params={'date': date_str},
            timeout=30
        )
        response.raise_for_status()
        
        customers = response.json()
        df = pd.DataFrame(customers)
        logging.info(f"Extracted {len(df)} customer records for {date_str}")
        
        # Save to local file system
        output_dir = f"/opt/data/raw/customers/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/customers.parquet"
        df.to_parquet(output_path)
        
        logging.info(f"Saved customer data to {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error extracting customer data: {e}")
        raise

def extract_product_data(**context):
    """Extract product data (simulated)"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Simulate product data
        products = [
            {"id": 1, "name": "Product A", "category": "Electronics", "price": 299.99},
            {"id": 2, "name": "Product B", "category": "Clothing", "price": 49.99},
            {"id": 3, "name": "Product C", "category": "Books", "price": 19.99},
            {"id": 4, "name": "Product D", "category": "Electronics", "price": 599.99},
            {"id": 5, "name": "Product E", "category": "Home", "price": 129.99}
        ]
        
        df = pd.DataFrame(products)
        logging.info(f"Generated {len(df)} product records")
        
        # Save to local file system
        output_dir = f"/opt/data/raw/products/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/products.parquet"
        df.to_parquet(output_path)
        
        logging.info(f"Saved product data to {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error extracting product data: {e}")
        raise

def validate_data_quality(**context):
    """Validate extracted data quality"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Check file existence and basic stats
        base_path = "/opt/data/raw"
        sales_path = f"{base_path}/sales/{date_str}/sales.parquet"
        customers_path = f"{base_path}/customers/{date_str}/customers.parquet"
        products_path = f"{base_path}/products/{date_str}/products.parquet"
        
        files_to_check = [sales_path, customers_path, products_path]
        
        for file_path in files_to_check:
            if not os.path.exists(file_path):
                raise ValueError(f"Required file not found: {file_path}")
            
            # Read and validate data
            df = pd.read_parquet(file_path)
            if len(df) == 0:
                raise ValueError(f"Empty dataset: {file_path}")
            
            file_size = os.path.getsize(file_path)
            logging.info(f"File: {file_path}, Records: {len(df)}, Size: {file_size} bytes")
        
        logging.info("Data quality validation passed")
        return True
        
    except Exception as e:
        logging.error(f"Data quality validation failed: {e}")
        raise

def process_data_with_pandas(**context):
    """Process data using Pandas (simulating Spark)"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Read data
        base_path = "/opt/data/raw"
        sales_df = pd.read_parquet(f"{base_path}/sales/{date_str}/sales.parquet")
        customers_df = pd.read_parquet(f"{base_path}/customers/{date_str}/customers.parquet")
        products_df = pd.read_parquet(f"{base_path}/products/{date_str}/products.parquet")
        
        logging.info(f"Loaded data - Sales: {len(sales_df)}, Customers: {len(customers_df)}, Products: {len(products_df)}")
        
        # Join data
        enriched_sales = sales_df.merge(customers_df, left_on='customer_id', right_on='id', how='left', suffixes=('', '_customer'))
        enriched_sales = enriched_sales.merge(products_df, left_on='product_id', right_on='id', how='left', suffixes=('', '_product'))
        
        # Perform aggregations
        daily_summary = enriched_sales.groupby(['segment', 'category']).agg({
            'amount': ['sum', 'mean', 'count'],
            'customer_id': 'nunique'
        }).reset_index()
        
        # Flatten column names
        daily_summary.columns = ['customer_segment', 'product_category', 'total_sales', 'avg_transaction_value', 'transaction_count', 'unique_customers']
        
        # Add derived metrics
        daily_summary['revenue_per_customer'] = daily_summary['total_sales'] / daily_summary['unique_customers']
        daily_summary['processing_date'] = date_str
        
        # Save processed data
        output_dir = f"/opt/data/processed/sales/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/daily_summary.parquet"
        daily_summary.to_parquet(output_path)
        
        logging.info(f"Processed data saved to {output_path}")
        logging.info(f"Summary records: {len(daily_summary)}")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

def load_to_warehouse(**context):
    """Load processed data to warehouse (PostgreSQL)"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Read processed data
        processed_path = f"/opt/data/processed/sales/{date_str}/daily_summary.parquet"
        df = pd.read_parquet(processed_path)
        
        # Connect to warehouse (using sales_db as warehouse for demo)
        pg_hook = PostgresHook(postgres_conn_id='sales_db_conn')
        
        # Insert data into warehouse
        for _, row in df.iterrows():
            insert_sql = """
                INSERT INTO sales_summary 
                (customer_segment, product_category, total_sales, avg_transaction_value, 
                 transaction_count, unique_customers, revenue_per_customer, processing_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_segment, product_category, processing_date) 
                DO UPDATE SET
                    total_sales = EXCLUDED.total_sales,
                    avg_transaction_value = EXCLUDED.avg_transaction_value,
                    transaction_count = EXCLUDED.transaction_count,
                    unique_customers = EXCLUDED.unique_customers,
                    revenue_per_customer = EXCLUDED.revenue_per_customer
            """
            pg_hook.run(insert_sql, parameters=tuple(row))
        
        logging.info(f"Loaded {len(df)} summary records to warehouse")
        return True
        
    except Exception as e:
        logging.error(f"Error loading to warehouse: {e}")
        raise

def generate_daily_report(**context):
    """Generate daily business report"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Connect to warehouse
        pg_hook = PostgresHook(postgres_conn_id='sales_db_conn')
        
        report_sql = """
            SELECT 
                customer_segment,
                product_category,
                total_sales,
                unique_customers,
                revenue_per_customer,
                transaction_count
            FROM sales_summary 
            WHERE processing_date = %s
            ORDER BY total_sales DESC
        """
        
        df = pg_hook.get_pandas_df(report_sql, parameters=[date_str])
        
        # Generate HTML report
        report_html = f"""
        <html>
        <head><title>Daily Sales Report - {date_str}</title></head>
        <body>
        <h1>Daily Sales Report - {date_str}</h1>
        <h2>Summary by Segment and Category</h2>
        {df.to_html(index=False, table_id='sales_table')}
        <h2>Key Metrics</h2>
        <ul>
        <li>Total Revenue: ${df['total_sales'].sum():,.2f}</li>
        <li>Total Customers: {df['unique_customers'].sum()}</li>
        <li>Total Transactions: {df['transaction_count'].sum()}</li>
        <li>Average Revenue per Customer: ${df['revenue_per_customer'].mean():,.2f}</li>
        </ul>
        </body>
        </html>
        """
        
        # Save report
        report_dir = f"/opt/data/reports/{date_str}"
        os.makedirs(report_dir, exist_ok=True)
        report_path = f"{report_dir}/daily_report.html"
        
        with open(report_path, 'w') as f:
            f.write(report_html)
        
        logging.info(f"Generated report: {report_path}")
        return report_path
        
    except Exception as e:
        logging.error(f"Error generating report: {e}")
        raise

def cleanup_temp_files(**context):
    """Clean up temporary files"""
    try:
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # In a real scenario, you might clean up files older than X days
        logging.info(f"Cleanup completed for {date_str}")
        return True
        
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")
        # Don't fail the DAG for cleanup errors
        return True

# Define tasks
extract_sales = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag
)

extract_customers = PythonOperator(
    task_id='extract_customer_data',
    python_callable=extract_customer_data,
    dag=dag
)

extract_products = PythonOperator(
    task_id='extract_product_data',
    python_callable=extract_product_data,
    dag=dag
)

validate_data = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data_with_pandas,
    dag=dag
)

load_warehouse = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag
)

cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done'
)

# Success notification
success_email = EmailOperator(
    task_id='send_success_notification',
    to=['data-team@company.com'],
    subject='Sales Pipeline Completed - {{ ds }}',
    html_content="""
    <h3>Sales Data Pipeline Completed Successfully</h3>
    <p>Date: {{ ds }}</p>
    <p>All tasks completed without errors.</p>
    <p>Report available at: /opt/data/reports/{{ ds }}/daily_report.html</p>
    """,
    dag=dag,
    trigger_rule='all_success'
)

# Define dependencies
[extract_sales, extract_customers, extract_products] >> validate_data
validate_data >> process_data
process_data >> load_warehouse
load_warehouse >> generate_report
generate_report >> success_email
[success_email, generate_report] >> cleanup