#!/bin/bash
# setup.sh - Setup script for Airflow E2E Pipeline

set -e

echo "🚀 Setting up Airflow E2E Data Pipeline..."

# Create directory structure
echo "📁 Creating directory structure..."
mkdir -p dags logs plugins config data/{raw,processed,reports} spark_jobs mock_services sample_data init-scripts spark_config

# Set environment variables
export AIRFLOW_UID=$(id -u)
export AIRFLOW_PROJ_DIR=$(pwd)

echo "AIRFLOW_UID=$AIRFLOW_UID" > .env
echo "AIRFLOW_PROJ_DIR=$AIRFLOW_PROJ_DIR" >> .env

# Create Airflow configuration
echo "⚙️ Creating Airflow configuration..."
cat > config/airflow.cfg << 'EOF'
[core]
dags_folder = /opt/airflow/dags
base_log_folder = /opt/airflow/logs
plugins_folder = /opt/airflow/plugins
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow
load_examples = False
max_active_runs_per_dag = 1

[webserver]
base_url = http://localhost:8081
web_server_port = 8080
secret_key = your-secret-key-here

[scheduler]
catchup_by_default = False

[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
EOF

# Create connections script
echo "🔗 Creating connections setup script..."
cat > setup_connections.py << 'EOF'
#!/usr/bin/env python3
"""Setup Airflow connections for the E2E pipeline"""

import subprocess
import sys
import time

def run_airflow_cmd(cmd):
    """Run airflow CLI command"""
    full_cmd = f"docker-compose exec -T airflow-webserver airflow {cmd}"
    print(f"Running: {full_cmd}")
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def setup_connections():
    """Setup all required connections"""
    connections = [
        {
            'conn_id': 'sales_db_conn',
            'conn_type': 'postgres',
            'host': 'sales_db',
            'port': '5432',
            'login': 'sales_user',
            'password': 'sales_password',
            'schema': 'sales_db'
        },
        {
            'conn_id': 'minio_default',
            'conn_type': 'aws',
            'host': 'http://minio:9000',
            'login': 'minioadmin',
            'password': 'minioadmin123'
        }
    ]
    
    for conn in connections:
        cmd_parts = [
            "connections add",
            f"--conn-id {conn['conn_id']}",
            f"--conn-type {conn['conn_type']}",
            f"--conn-host {conn['host']}",
            f"--conn-port {conn['port']}" if 'port' in conn else "",
            f"--conn-login {conn['login']}" if 'login' in conn else "",
            f"--conn-password {conn['password']}" if 'password' in conn else "",
            f"--conn-schema {conn['schema']}" if 'schema' in conn else ""
        ]
        
        cmd = " ".join(filter(None, cmd_parts))
        run_airflow_cmd(cmd)

if __name__ == "__main__":
    print("⏳ Waiting for Airflow to be ready...")
    time.sleep(30)
    print("🔗 Setting up connections...")
    setup_connections()
    print("✅ Connections setup complete!")
EOF

chmod +x setup_connections.py

# Create requirements file
echo "📦 Creating requirements.txt..."
cat > requirements.txt << 'EOF'
apache-airflow[postgres,http,amazon]==2.8.1
pandas==2.0.3
boto3==1.34.0
requests==2.31.0
pyarrow==14.0.0
psycopg2-binary==2.9.9
flask==3.0.0
EOF

echo "🐳 Starting Docker Compose services..."
echo "This may take a few minutes for the first run..."

# Start services
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 60

# Setup connections
echo "🔗 Setting up Airflow connections..."
python3 setup_connections.py

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "📊 Access URLs:"
echo "  - Airflow UI: http://localhost:8081 (admin/airflow)"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "  - Customer API: http://localhost:5000/customers"
echo ""
echo "🔧 Useful commands:"
echo "  - View logs: docker-compose logs -f [service_name]"
echo "  - Stop services: docker-compose down"
echo "  - Restart services: docker-compose restart"
echo "  - Access Airflow CLI: docker-compose exec airflow-webserver airflow [command]"
echo ""
echo "📝 Next steps:"
echo "  1. Open Airflow UI at http://localhost:8081"
echo "  2. Login with admin/airflow"
echo "  3. Enable the 'sales_data_pipeline' DAG"
echo "  4. Trigger a manual run to test the pipeline"
echo ""