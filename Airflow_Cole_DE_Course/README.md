# Airflow E2E Data Pipeline Demo

Complete end-to-end data pipeline demonstration using Apache Airflow with Docker Compose.

## Features

- **Complete Docker Setup**: All services containerized and ready to run
- **Mock Data Sources**: PostgreSQL database and REST API for realistic data extraction
- **Data Processing**: ETL pipeline with data validation and transformation
- **Storage**: File-based storage simulating S3/Data Lake
- **Monitoring**: Airflow UI for pipeline monitoring and management
- **Reporting**: Automated report generation

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ PostgreSQL  │    │  REST API   │    │  Products   │
│   (Sales)   │    │ (Customers) │    │ (Simulated) │
└─────────────┘    └─────────────┘    └─────────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    ┌─────────────┐
                    │   Airflow   │
                    │ Orchestrator│
                    └─────────────┘
                           │
                    ┌─────────────┐
                    │    Pandas   │
                    │ Processing  │
                    └─────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Reports   │    │  Warehouse  │    │  Data Lake  │
│   (HTML)    │    │(PostgreSQL) │    │   (Files)   │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Quick Start

1. **Clone and Setup**
   ```bash
   git clone <repository>
   cd airflow-e2e-pipeline
   chmod +x setup.sh
   ./setup.sh
   ```

2. **Access Services**
   - Airflow UI: http://localhost:8081 (admin/airflow)
   - Spark Master: http://localhost:8080
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)

3. **Run Pipeline**
   - Open Airflow UI
   - Navigate to DAGs
   - Enable `sales_data_pipeline`
   - Click "Trigger DAG" to run manually

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Airflow Web | 8081 | Pipeline management UI |
| Spark Master | 8080 | Spark cluster monitoring |
| MinIO Console | 9001 | S3-compatible storage UI |
| PostgreSQL | 5432 | Airflow metadata |
| Sales DB | 5433 | Source database |
| Customer API | 5000 | Mock REST API |

## Data Flow

1. **Extract**: Pull data from PostgreSQL, REST API, and generate product data
2. **Validate**: Check data quality and completeness
3. **Transform**: Join and aggregate data using Pandas
4. **Load**: Insert results into data warehouse
5. **Report**: Generate HTML reports
6. **Cleanup**: Remove temporary files

## Configuration

### Airflow Connections
The setup script automatically creates these connections:
- `sales_db_conn`: PostgreSQL connection for sales database
- `minio_default`: MinIO S3-compatible storage connection

### Environment Variables
Key variables in `.env`:
- `AIRFLOW_UID`: User ID for Airflow containers
- `AIRFLOW_PROJ_DIR`: Project directory path

## Monitoring

### Airflow UI Features
- **DAG View**: Visual representation of pipeline
- **Task Logs**: Detailed execution logs
- **Gantt Chart**: Task timing visualization
- **Graph View**: Task dependencies

### Log Locations
- Airflow logs: `./logs/`
- Service logs: `docker-compose logs [service]`

## Development

### Adding New DAGs
1. Create Python file in `dags/` directory
2. Define DAG with appropriate schedule
3. Add required connections in Airflow UI

### Customizing Pipeline
- Modify `dags/sales_pipeline_dag.py` for business logic
- Update `sample_data/01_create_tables.sql` for schema changes
- Adjust `mock_services/customer_api.py` for API responses

## Troubleshooting

### Common Issues
1. **Services not starting**: Check Docker resources and ports
2. **Connection errors**: Verify connection configuration in Airflow
3. **Permission errors**: Ensure proper file permissions with `chmod -R 755`

### Debug Commands
```bash
# Check service status
docker-compose ps

# View service logs
docker-compose logs -f airflow-scheduler

# Access Airflow CLI
docker-compose exec airflow-webserver airflow dags list

# Restart specific service
docker-compose restart airflow-scheduler
```

## Production Considerations

For production deployment:
- Use external PostgreSQL/Redis
- Implement proper secrets management
- Set up SSL/TLS
- Configure proper logging and monitoring
- Use Kubernetes executor for scalability
- Implement data backup strategies

## License

MIT License - see LICENSE file for details
```

## 🚀 Usage Instructions

1. **Setup**: Run `./setup.sh` to initialize everything
2. **Access**: Open http://localhost:8081 in your browser
3. **Login**: Use admin/airflow credentials
4. **Enable DAG**: Toggle the `sales_data_pipeline` DAG
5. **Run**: Click "Trigger DAG" to execute the pipeline
6. **Monitor**: Watch tasks execute in real-time
7. **Check Results**: View generated reports in `/data/reports/`

## 🔧 Customization Options

- **Data Sources**: Modify connection strings and queries
- **Processing Logic**: Update transformation functions
- **Scheduling**: Change DAG schedule_interval
- **Notifications**: Configure email settings
- **Storage**: Switch to actual S3 instead of local files