# terraform/main.tf
# Terraform configuration for Google Cloud infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "sql.googleapis.com",
    "cloudbuild.googleapis.com",
    "secretmanager.googleapis.com",
    "vpcaccess.googleapis.com",
    "storage.googleapis.com"
  ])
  
  service = each.key
  disable_dependent_services = false
}

# Cloud SQL PostgreSQL instance
resource "google_sql_database_instance" "postgres" {
  name             = "blockchain-indexer-${var.environment}"
  database_version = "POSTGRES_14"
  region          = var.region
  
  settings {
    tier      = "db-custom-2-8192"  # 2 vCPU, 8GB RAM
    disk_size = 100                  # 100GB SSD
    disk_type = "PD_SSD"
    
    backup_configuration {
      enabled                        = true
      start_time                    = "03:00"
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
    }
    
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }
    
    database_flags {
      name  = "max_connections"
      value = "200"
    }
    
    database_flags {
      name  = "shared_preload_libraries"
      value = "pg_stat_statements"
    }
  }
  
  deletion_protection = true
}

# Database and user
resource "google_sql_database" "database" {
  name     = "blockchain_indexer"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "user" {
  name     = "indexer_app"
  instance = google_sql_database_instance.postgres.name
  password = random_password.db_password.result
}

resource "random_password" "db_password" {
  length  = 32
  special = true
}

# VPC for private database access
resource "google_compute_network" "vpc" {
  name                    = "indexer-vpc-${var.environment}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "indexer-subnet-${var.environment}"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
}

# VPC connector for Cloud Run
resource "google_vpc_access_connector" "connector" {
  name          = "indexer-connector"
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.1.0.0/28"
}

# Private service connection for Cloud SQL
resource "google_compute_global_address" "private_ip_address" {
  name          = "private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

# GCS bucket for blockchain data
resource "google_storage_bucket" "blockchain_data" {
  name     = "${var.project_id}-blockchain-data"
  location = "US"
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Service account for Cloud Run
resource "google_service_account" "cloud_run_sa" {
  account_id   = "blockchain-indexer"
  display_name = "Blockchain Indexer Service Account"
}

# IAM bindings for service account
resource "google_project_iam_member" "cloud_run_sa_roles" {
  for_each = toset([
    "roles/cloudsql.client",
    "roles/storage.admin",
    "roles/secretmanager.secretAccessor"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# Secrets for sensitive configuration
resource "google_secret_manager_secret" "database_config" {
  secret_id = "database-config"
  
  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "database_config" {
  secret = google_secret_manager_secret.database_config.id
  
  secret_data = jsonencode({
    host     = google_sql_database_instance.postgres.private_ip_address
    username = google_sql_user.user.name
    password = google_sql_user.user.password
    database = google_sql_database.database.name
  })
}

resource "google_secret_manager_secret" "api_keys" {
  secret_id = "api-keys"
  
  replication {
    automatic = true
  }
}

# You need to manually set this secret with your RPC endpoint
resource "google_secret_manager_secret_version" "api_keys" {
  secret = google_secret_manager_secret.api_keys.id
  
  secret_data = jsonencode({
    rpc-endpoint = "REPLACE_WITH_YOUR_RPC_ENDPOINT"
  })
  
  lifecycle {
    ignore_changes = [secret_data]
  }
}

# Outputs
output "database_connection_name" {
  value = google_sql_database_instance.postgres.connection_name
}

output "database_private_ip" {
  value = google_sql_database_instance.postgres.private_ip_address
}

output "vpc_connector_name" {
  value = google_vpc_access_connector.connector.name
}

output "service_account_email" {
  value = google_service_account.cloud_run_sa.email
}