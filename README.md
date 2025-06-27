# GCP SETUP
Development: Use Cloud SQL Proxy → connect to ```localhost:5432```
Production: Connect directly to Cloud SQL IP → connect to ```CLOUD_SQL_IP:5432```
Interactive SQL client: ```gcloud sql connect```

## Development
- Authenticate: ```gcloud auth application-default login```
- Check port 5432: ```lsof -i :5432```
- Verify Cloud SQL instance: ```gcloud sql instances list```
- Set env variables if not already present
```
export INDEXER_DB_HOST=127.0.0.1
export INDEXER_DB_PORT=5432
```
- Cloud SQL Proxy (recommended for development)
    - Secure tunnel to Cloud SQL through Google's infra
    - Start proxy(v2): ```cloud-sql-proxy --port 5432 $INDEXER_SQL_CONNECTION_NAME```
    - Start proxy(v1): ```cloud_sql_proxy -instances=$INDEXER_SQL_CONNECTION_NAME=tcp:5432```

## Production
- Connect directly to Cloud SQL's public/private IP
- Database host stored in GCP Secrets: ```indexer_db_host```
- Set env variables if not already present
```
export INDEXER_DB_HOST=CLOUD_SQL_IP # From secrets or config
export INDEXER_DB_PORT=5432
```
- Ensure production environment's IP is in Cloud SQL's authorized networks
- May use private IP if deployed in same VPC/network

## Interactive SQL Session
- Interactive psql session - not for applications/indexer
- Using gcloud CLI: ```gcloud sql connect $INDEXER_SQL_INSTANCE_NAME```
