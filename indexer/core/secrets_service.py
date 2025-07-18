# indexer/core/secrets_service.py

from google.cloud import secretmanager
from typing import Optional

from .logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class SecretsService:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        self.logger = IndexerLogger.get_logger('core.secrets')
    
    def get_secret(self, secret_name: str, version: str = "latest") -> Optional[str]:
        try:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8")
            
            log_with_context(self.logger, DEBUG, "Secret retrieved successfully",
                           secret_name=secret_name, project_id=self.project_id)
            
            return secret_value
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Failed to retrieve secret",
                           secret_name=secret_name, project_id=self.project_id, error=str(e))
            return None
    
    def get_database_credentials(self) -> dict:
        credentials = {}
        
        secret_mapping = {
            'user': 'indexer-db-user',
            'password': 'indexer-db-password',
            'host': 'indexer-db-host',  # Optional - might use env for local dev
        }
        
        for field, secret_name in secret_mapping.items():
            value = self.get_secret(secret_name)
            if value:
                credentials[field] = value
        
        log_with_context(self.logger, INFO, "Database credentials retrieved",
                       credential_fields=list(credentials.keys()))
        
        return credentials
    
    def get_rpc_endpoint(self) -> Optional[str]:
        return self.get_secret('quicknode-avalanche-mainnet-rpc')