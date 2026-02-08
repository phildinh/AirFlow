-- sample_data/01_create_tables.sql
-- Create sales table
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sales summary table (data warehouse)
CREATE TABLE IF NOT EXISTS sales_summary (
    id SERIAL PRIMARY KEY,
    customer_segment VARCHAR(50) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    total_sales DECIMAL(15,2) NOT NULL,
    avg_transaction_value DECIMAL(10,2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    unique_customers INTEGER NOT NULL,
    revenue_per_customer DECIMAL(10,2) NOT NULL,
    processing_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_segment, product_category, processing_date)
);

-- Insert sample sales data
INSERT INTO sales (customer_id, product_id, amount, quantity, created_at) VALUES
(1, 1, 299.99, 1, '2024-01-01 10:00:00'),
(2, 2, 49.99, 2, '2024-01-01 10:30:00'),
(3, 3, 19.99, 1, '2024-01-01 11:00:00'),
(1, 4, 599.99, 1, '2024-01-01 11:30:00'),
(4, 5, 129.99, 1, '2024-01-01 12:00:00'),
(5, 1, 299.99, 1, '2024-01-01 12:30:00'),
(2, 3, 19.99, 3, '2024-01-01 13:00:00'),
(3, 2, 49.99, 1, '2024-01-01 13:30:00'),
(4, 4, 599.99, 1, '2024-01-01 14:00:00'),
(5, 5, 129.99, 2, '2024-01-01 14:30:00'),
-- Add more sample data for different dates
(1, 2, 49.99, 1, CURRENT_DATE),
(2, 1, 299.99, 1, CURRENT_DATE),
(3, 5, 129.99, 1, CURRENT_DATE),
(4, 3, 19.99, 2, CURRENT_DATE),
(5, 4, 599.99, 1, CURRENT_DATE);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sales_created_at ON sales(created_at);
CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_summary_date ON sales_summary(processing_date);