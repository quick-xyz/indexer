# Google Cloud SQL Setup via Console

This guide walks you through setting up PostgreSQL using the Google Cloud Console web interface.

## Step 1: Access Cloud SQL

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Make sure you're in the same project where your GCS bucket is located
3. In the left sidebar, go to **SQL** (or search for "SQL" in the top search bar)
4. If prompted, enable the Cloud SQL Admin API by clicking **Enable**

## Step 2: Create SQL Instance

1. Click **Create Instance**
2. Choose **PostgreSQL**
3. Fill in the instance details:

### Basic Information
- **Instance ID**: `indexer-postgres` (or your preferred name)
- **Password**: Set a strong password for the `postgres` user
- **Database Version**: PostgreSQL 15 (recommended)
- **Region**: Choose the same region as your GCS bucket if possible
- **Zonal availability**: Single zone (for development) or Multiple zones (for production)

### Machine Configuration
- **Machine type**: 
  - Development: **Shared core** → **1 vCPU, 0.614 GB**
  - Production: **Standard** → **1 vCPU, 3.75 GB** or higher

### Storage
- **Storage type**: SSD
- **Storage capacity**: 10 GB (will auto-increase)
- ✅ **Enable automatic storage increases**

### Connections
- **Public IP**: ✅ Enabled (for now - we'll secure this)
- **Private IP**: Leave unchecked for now
- **Authorized networks**: Leave empty for now

### Backup and Recovery
- ✅ **Enable automated backups**
- **Backup window**: Choose a low-usage time (e.g., 3:00 AM)
- **Backup location**: Same region as instance

### Maintenance
- **Maintenance window**: Choose a day/time for updates (e.g., Sunday 4:00 AM)
- **Order of update**: Earlier is fine

4. Click **Create Instance** (this takes 5-10 minutes)

## Step 3: Create Database and User

Once your instance is ready:

### Create the Database
1. Click on your instance name (`indexer-postgres`)
2. Go to **Databases** tab
3. Click **Create Database**
4. **Database name**: `indexer_prod`
5. Click **Create**

### Create Application User
1. Go to **Users** tab
2. Click **Add User Account**
3. **Username**: `indexer_user`
4. **Password**: Create a strong password for your application
5. Click **Add**

## Step 4: Configure Network Access

### Option A: Authorize Your IP (Quick Setup)
1. In your instance, go to **Connections** tab
2. Under **Authorized networks**, click **Add Network**
3. **Name**: `My Development Machine`
4. **Network**: Go to [whatismyip.com](https://whatismyip.com) and enter your IP address
5. Add `/32` to the end (e.g., `123.456.789.012/32`)
6. Click **Done** then **Save**

### Option B: Use Cloud SQL Proxy (Recommended)
1. No network configuration needed in console
2. Download and run the proxy locally (see commands below)

## Step 5: Grant Database Permissions

### Connect to Your Database
1. In your instance overview, click **Connect using Cloud Shell**
2. This opens a terminal in your browser
3. Run: `gcloud sql connect indexer-postgres --user=postgres`
4. Enter the postgres password you set earlier

### Grant Permissions
Run these SQL commands:
```sql
-- Grant all privileges to your application user
GRANT ALL PRIVILEGES ON DATABASE indexer_prod TO indexer_user;

-- Connect to the application database
\c indexer_prod

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO indexer_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO indexer_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO indexer_user;

-- Grant future permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO indexer_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO indexer_user;

-- Exit
\q
```

## Step 6: Get Connection Information

### Get Your Connection Details
1. In your SQL instance overview page, note:
   - **Public IP address** (if using direct connection)
   - **Connection name** (looks like `project:region:instance`)

### If Using Cloud SQL Proxy (Recommended)
Download and run the proxy on your local machine:

```bash
# Download for macOS
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.amd64

# Download for Linux
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64

# Make executable
chmod +x cloud_sql_proxy

# Run the proxy (replace YOUR_CONNECTION_NAME)
./cloud_sql_proxy -instances=YOUR_CONNECTION_NAME=tcp:5432
```

Keep this running in a separate terminal window.

## Step 7: Configure Your Environment Variables

Add these to your `.env` file or export them:

### If using Cloud SQL Proxy:
```bash
export INDEXER_DB_USER=indexer_user
export INDEXER_DB_PASSWORD=your-app-user-password
export INDEXER_DB_NAME=indexer_prod
export INDEXER_DB_HOST=127.0.0.1
export INDEXER_DB_PORT=5432
```

### If using direct connection:
```bash
export INDEXER_DB_USER=indexer_user
export INDEXER_DB_PASSWORD=your-app-user-password
export INDEXER_DB_NAME=indexer_prod
export INDEXER_DB_HOST=your-public-ip-from-console
export INDEXER_DB_PORT=5432
```

## Step 8: Test Your Connection

Run your migration to test:
```bash
python -m indexer.database.migrate create "Initial tables and indexes"
```

## Security Notes

1. **For Production**: 
   - Remove public IP and use private IP
   - Use Cloud SQL Proxy or VPC peering
   - Enable SSL connections
   - Use IAM database authentication

2. **For Development**:
   - Cloud SQL Proxy is the easiest and most secure option
   - Authorized networks work but require IP updates if your IP changes

3. **Cost Management**:
   - The `db-f1-micro` instance costs ~$7/month
   - You can stop the instance when not developing to save costs
   - Storage is additional (~$0.17/GB/month)

## Troubleshooting

### Can't Connect?
- Check if your IP is authorized (if not using proxy)
- Verify the instance is running (green status in console)
- Check firewall rules if using direct connection
- Ensure passwords are correct

### Permission Errors?
- Make sure you granted permissions in Step 5
- Verify the user exists in the Users tab
- Check that the database exists in the Databases tab

### Need to Reset?
- You can delete and recreate users in the Users tab
- You can reset the postgres password in the Users tab
- You can delete and recreate the database in the Databases tab