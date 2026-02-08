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
