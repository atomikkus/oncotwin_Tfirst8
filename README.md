# OncoTwin API

A FastAPI-based REST API for processing patient data and generating twin matching results with batch processing capabilities.

## Features

- **Batch Processing**: Process multiple doctor-patient lists concurrently
- **Job Tracking**: Track job status and retrieve results asynchronously
- **Multiple Output Formats**: Get results in JSON or Excel format
- **Caching**: In-memory caching for improved performance
- **API Key Authentication**: Secure token-based access control
- **Health Monitoring**: Health check endpoint for monitoring

## Prerequisites

- Python 3.11+
- Docker and Docker Compose (optional, for containerized deployment)

## Installation

### Local Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd oncotwin_Tfirst8
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment**
   - Windows:
     ```bash
     venv\Scripts\activate
     ```
   - Linux/Mac:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Set up environment variables** (optional)
   Create a `.env` file in the root directory:
   ```env
   API_KEY=your_custom_api_key_here
   USER_EMAIL=your_email@example.com
   USER_PASSWORD=your_password
   ECRF_EMAIL=ecrf_email@example.com
   ECRF_PASSWORD=ecrf_password
   ```

## Running the API

### Local Development

```bash
python -m uvicorn src.otwin8_api:app --host 0.0.0.0 --port 8002
```

Or directly:
```bash
python src/otwin8_api.py
```

The API will be available at `http://localhost:8002`

### Docker Deployment

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Or build and run manually**
   ```bash
   docker build -t oncotwin-api .
   docker run -p 8002:8002 oncotwin-api
   ```

## Authentication

The API uses API key authentication via the `X-API-Key` header.

### Default API Key

The default API key is generated from the credentials:
- **Username**: `satya@4basacare.com`
- **Password**: `ocWin@43321!`
- **API Key**: `aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882`

### Custom API Key

Set a custom API key using the `API_KEY` environment variable:
```bash
export API_KEY=your_custom_key_here
```

### Using Authentication

Include the API key in all requests (except `/health`):

```bash
curl -H "X-API-Key: aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882" \
     http://localhost:8002/
```

## API Endpoints

### Base URL
```
http://localhost:8002
```

### Endpoints

#### `GET /`
Get API information and version.

**Authentication**: Required

**Response**:
```json
{
  "message": "OncoTwin Simplified API",
  "version": "1.0.0",
  "features": ["Redis caching", "Excel/JSON output", "Job tracking"]
}
```

#### `GET /health`
Health check endpoint (no authentication required).

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00"
}
```

#### `POST /process`
Submit a batch processing job.

**Authentication**: Required

**Request Body**:
```json
{
  "requests": [
    {
      "doctor_id": 123,
      "patient_ids": ["PAT001", "PAT002", "PAT003"]
    },
    {
      "doctor_id": 456,
      "patient_ids": ["PAT004", "PAT005"]
    }
  ],
  "refresh": false
}
```

**Response**:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Batch job started.",
  "status": "pending"
}
```

#### `GET /status/{job_id}`
Get the status of a processing job.

**Authentication**: Required

**Response**:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "message": "Processed 2 of 2 lists successfully.",
  "created_at": "2024-01-01T12:00:00",
  "completed_at": "2024-01-01T12:05:00",
  "doctor_ids_total": [123, 456],
  "doctor_ids_success": [123, 456],
  "doctor_ids_failed": []
}
```

**Status Values**:
- `pending`: Job is queued
- `running`: Job is being processed
- `completed`: Job finished successfully
- `failed`: Job failed

#### `GET /job/{job_id}/details`
Get detailed debug information about a job (includes error details).

**Authentication**: Required

#### `GET /results/{job_id}`
Get results in JSON format.

**Authentication**: Required

**Response**: JSON object containing match results

#### `GET /download/{job_id}/{format}`
Download results file.

**Authentication**: Required

**Parameters**:
- `job_id`: Job identifier
- `format`: `json` or `excel`

**Example**:
```bash
curl -H "X-API-Key: your_api_key" \
     http://localhost:8002/download/550e8400-e29b-41d4-a716-446655440000/excel \
     --output results.xlsx
```

#### `DELETE /job/{job_id}`
Delete a job and its status.

**Authentication**: Required

#### `GET /cache/clear`
Clear all cached results.

**Authentication**: Required

#### `GET /cache/stats`
Get cache statistics.

**Authentication**: Required

**Response**:
```json
{
  "caching_backend": "in_memory",
  "memory_cache_size": 10,
  "job_status_size": 5
}
```

## Usage Examples

### Python

```python
import requests

API_KEY = "aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882"
BASE_URL = "http://localhost:8002"
headers = {"X-API-Key": API_KEY}

# Submit a job
payload = {
    "requests": [
        {
            "doctor_id": 123,
            "patient_ids": ["PAT001", "PAT002"]
        }
    ],
    "refresh": False
}

response = requests.post(f"{BASE_URL}/process", json=payload, headers=headers)
job_data = response.json()
job_id = job_data["job_id"]
print(f"Job ID: {job_id}")

# Check status
status_response = requests.get(f"{BASE_URL}/status/{job_id}", headers=headers)
print(status_response.json())

# Get results
results_response = requests.get(f"{BASE_URL}/results/{job_id}", headers=headers)
results = results_response.json()
print(results)
```

### cURL

```bash
# Submit a job
curl -X POST http://localhost:8002/process \
  -H "X-API-Key: aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882" \
  -H "Content-Type: application/json" \
  -d '{
    "requests": [
      {
        "doctor_id": 123,
        "patient_ids": ["PAT001", "PAT002"]
      }
    ],
    "refresh": false
  }'

# Check status
curl -H "X-API-Key: aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882" \
     http://localhost:8002/status/{job_id}

# Download results
curl -H "X-API-Key: aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882" \
     http://localhost:8002/download/{job_id}/excel \
     --output results.xlsx
```

### JavaScript/Fetch

```javascript
const API_KEY = "aa6be4b57c3032de16daef63e1229230020e2d3e6bf4bb585d3f356f6531d882";
const BASE_URL = "http://localhost:8002";

// Submit a job
const payload = {
  requests: [
    {
      doctor_id: 123,
      patient_ids: ["PAT001", "PAT002"]
    }
  ],
  refresh: false
};

fetch(`${BASE_URL}/process`, {
  method: "POST",
  headers: {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
  },
  body: JSON.stringify(payload)
})
  .then(res => res.json())
  .then(data => {
    console.log("Job ID:", data.job_id);
    // Check status
    return fetch(`${BASE_URL}/status/${data.job_id}`, {
      headers: { "X-API-Key": API_KEY }
    });
  })
  .then(res => res.json())
  .then(status => console.log("Status:", status));
```

## Configuration

### Environment Variables

- `API_KEY`: Custom API key (defaults to generated key from credentials)
- `USER_EMAIL`: User email for data access
- `USER_PASSWORD`: User password for data access
- `ECRF_EMAIL`: ECRF email for data access
- `ECRF_PASSWORD`: ECRF password for data access

### Configuration Files

- `config/column_subsets.json`: Column configuration
- `config/gene_pathways_kegg.json`: Gene pathway mappings
- `config/weights.json`: Scoring weights

## Project Structure

```
oncotwin_Tfirst8/
├── src/
│   ├── otwin8_api.py          # Main API application
│   ├── run_pipeline_pq.py      # Pipeline execution
│   ├── twin_algo_pq.py         # Twin matching algorithm
│   ├── ecrf_extract_pq.py      # ECRF data extraction
│   └── workbench_retrieval.py  # Workbench data retrieval
├── config/                     # Configuration files
├── api_outputs/                 # Output directory for results
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose configuration
└── README.md                   # This file
```

## API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8002/docs`
- ReDoc: `http://localhost:8002/redoc`

## Error Handling

The API returns standard HTTP status codes:

- `200`: Success
- `400`: Bad Request (invalid input)
- `401`: Unauthorized (missing or invalid API key)
- `404`: Not Found (job or resource not found)
- `500`: Internal Server Error

Error responses include a `detail` field with error information:
```json
{
  "detail": "Invalid API key"
}
```

## Notes

- Jobs are processed asynchronously in the background
- Results are cached in memory for improved performance
- The `/health` endpoint does not require authentication
- All other endpoints require the `X-API-Key` header
- Job results are stored in the `api_outputs/` directory



