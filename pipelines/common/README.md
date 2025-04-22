# GCP Authentication System

This directory contains utilities for Google Cloud Platform (GCP) authentication used across all pipelines in the travel data platform.

## Overview

The authentication system provides a standardized way to handle GCP credentials across all pipelines, with support for different authentication methods and a command-line interface for easy setup.

## Key Components

- **gcp_auth.py**: Core authentication utilities
- **setup_credentials.py**: Command-line tool for credential setup

## How It Works

### Credential Sources (Priority Order)

The system looks for credentials in the following order:

1. Explicitly provided path in code
2. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. Default location (`~/.gcp/service-account-key.json`)

If no credentials are found, a descriptive `FileNotFoundError` is raised.

### Basic Usage

```python
from pipelines.common.gcp_auth import get_credentials, get_bigquery_client

# Get credentials
credentials = get_credentials()

# Get BigQuery client
client = get_bigquery_client()

# Write to BigQuery
client.query("SELECT 1")
```

## Setting Up Credentials

### Using the Command-Line Tool

The `setup_credentials.py` script provides several options:

```bash
# Show help
python pipelines/common/setup_credentials.py

# Import a service account key
python pipelines/common/setup_credentials.py --key-file /path/to/key.json

# Set environment variable for current session
python pipelines/common/setup_credentials.py --env-var

# Verify credentials
python pipelines/common/setup_credentials.py --verify

# Combine options
python pipelines/common/setup_credentials.py --key-file /path/to/key.json --env-var --verify
```

### Manual Setup

1. Save your service account key to `~/.gcp/service-account-key.json`
2. Set the environment variable:
   - Linux/Mac: `export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/service-account-key.json`
   - Windows: `setx GOOGLE_APPLICATION_CREDENTIALS "C:\Users\<user>\.gcp\service-account-key.json"`

## Common Scenarios

### First-Time Setup

```bash
# Import key and verify
python pipelines/common/setup_credentials.py --key-file /path/to/key.json --verify
```

### Missing Credentials

If you run a pipeline without credentials, you'll see an error like:

```
FileNotFoundError: No GCP credentials found. Please either:
1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable
2. Place credentials at ~/.gcp/service-account-key.json
3. Provide an explicit key_path parameter
```

To fix this, use one of the setup methods described above.

### Switching Projects

To use a different GCP project:

```python
# Explicitly provide a project ID
client = get_bigquery_client(project_id="different-project-id")
```

### Verifying Credentials

To check if your credentials are working:

```bash
python pipelines/common/setup_credentials.py --verify
```

This will attempt to list datasets in your project and indicate success or failure.

### Using Different Locations

By default, BigQuery clients use the "US" location. To specify a different location:

```python
client = get_bigquery_client(location="europe-west1")
```

## Security Considerations

- Service account keys are stored with restrictive permissions (only owner can read/write)
- Never commit service account keys to version control
- Consider using more secure authentication methods in production (like Workload Identity) 