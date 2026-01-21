# Data-pipeline-Bigquery-to-sftp-server
This data pipeline downloads csv files from an sftp server and uploads them into bigquery and likewise downloads data from bigquery tables and uploads them into the sftp server

# Impumelelo SFTP Upload

A Python Cloud Function that syncs data between an SFTP server, Google Sheets, and BigQuery.

## Features

- **SFTP to BigQuery**: Downloads CSV files from an SFTP server and uploads them to BigQuery
- **Google Sheets to SFTP**: Exports Google Sheet tabs as CSV files and uploads them to SFTP

## Prerequisites

- Python 3.11+
- Google Cloud Project with BigQuery enabled
- SFTP server access
- Google Cloud service account with BigQuery and Google Sheets permissions

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-org/impumelelo-sftp-upload.git
cd impumelelo-sftp-upload
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root (this file is gitignored):

```bash
# SFTP credentials (as JSON)
IMPUMELELO_CREDS='{"SFTP_HOST": "your-sftp-host", "SFTP_PORT": "22", "SFTP_USER": "your-username", "SFTP_PASS": "your-password"}'

# BigQuery service account credentials (as JSON)
BIGQUERY_CREDS='{"type": "service_account", "project_id": "your-project", ...}'

# Optional: Google Sheet URL for custom file uploads
IMPUMELELO_SHEET_URL=https://docs.google.com/spreadsheets/d/your-sheet-id
```

#### IMPUMELELO_CREDS

JSON object containing SFTP connection details:

| Key | Description |
|-----|-------------|
| `SFTP_HOST` | SFTP server hostname |
| `SFTP_PORT` | SFTP port (default: 22) |
| `SFTP_USER` | SFTP username |
| `SFTP_PASS` | SFTP password |

#### BIGQUERY_CREDS

Your Google Cloud service account JSON key. The service account needs:
- `BigQuery Data Editor` role
- `BigQuery Job User` role
- (Optional) `Google Sheets API` access if using the Sheets integration

### 4. Update BigQuery configuration

Edit `main.py` to set your BigQuery project and dataset:

```python
PROJECT_ID = "your-project-id"
DATASET_ID = "your-dataset"
```

## Usage

### Local development

```bash
python main.py
```

### Deploy to Google Cloud Functions

The included GitHub Actions workflow (`.github/workflows/build-and-deploy.yml`) automatically deploys to Cloud Functions on push to `main`.

Required GitHub Secrets:
- `GCP_DOCKER_CLOUDRUN`: Service account JSON for deployment

Required GCP Secret Manager secrets:
- `BIGQUERY_CREDS`: BigQuery service account credentials
- `IMPUMELELO_CREDS`: SFTP credentials

## Project Structure

```
.
├── main.py              # Main Cloud Function entry point
├── config.py            # Configuration and credential loading
├── requirements.txt     # Python dependencies
└── .github/workflows/   # CI/CD pipeline
```

## Data Flow

1. **Download from SFTP**: Reads `Overall_stats_live_manual_*.csv` and `Overall_stats_runners_*.csv` from the SFTP outgoing directory
2. **Upload to BigQuery**: Processes and uploads data to the configured BigQuery tables
3. **Sheets to SFTP** (optional): Exports the latest Google Sheet tab as CSV to the SFTP incoming directory

## Security Notes

- Never commit `.env` files or service account JSON files
- CSV data files are gitignored as they may contain PII
- All credentials should be stored in environment variables or secret managers

## License

MIT

