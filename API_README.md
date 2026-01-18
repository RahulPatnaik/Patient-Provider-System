# KPME Provider Validation API

FastAPI-based REST API for validating healthcare providers in Karnataka, India using the KPME database.

## Quick Start

### 1. Start the API Server

```bash
# From project root
python src/main.py
```

The API will start on `http://localhost:8000`

### 2. Access Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

### 3. Test the API

```bash
# Run the test suite
python test_api.py
```

## API Endpoints

### Provider Validation

#### 1. Fast Certificate Validation (Deterministic, Ultra-Fast)

**POST** `/api/v1/providers/validate/fast`

Ultra-fast KPME certificate validation (0 AI calls, 2,306 validations/sec).

```bash
curl -X POST "http://localhost:8000/api/v1/providers/validate/fast" \
  -H "Content-Type: application/json" \
  -d '{
    "certificate_number": "BDR00172ALHL2",
    "provider_name": "AAROGYA HOSPITAL",
    "use_cache": true
  }'
```

**Response:**
```json
{
  "request_id": "req_abc123",
  "timestamp": "2026-01-04T19:30:00Z",
  "is_valid": true,
  "certificate_number": "BDR00172ALHL2",
  "establishment_name": "AAROGYA HOSPITAL",
  "is_expired": false,
  "confidence": 0.9,
  "execution_time_ms": 2,
  "cache_hit": true
}
```

#### 2. Complete Provider Validation (AI-Powered Synthesis)

**POST** `/api/v1/providers/validate`

Full validation workflow with multi-source synthesis.

```bash
curl -X POST "http://localhost:8000/api/v1/providers/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "establishment_name": "AAROGYA HOSPITAL",
    "certificate_number": "BDR00172ALHL2",
    "phone": "9448452147",
    "district": "Bangalore Urban"
  }'
```

**Response:**
```json
{
  "request_id": "req_def456",
  "timestamp": "2026-01-04T19:30:00Z",
  "decision": "auto_approved",
  "validation_path": "simple",
  "final_confidence": 0.92,
  "confidence_level": "very_high",
  "provider_data": { ... },
  "kpme_data": { ... },
  "data_quality": { ... },
  "compliance": { ... },
  "execution_time_ms": 450,
  "cache_hit": false,
  "reasoning": [
    "KPME certificate valid",
    "High data quality"
  ]
}
```

#### 3. Batch Validation (Parallel Processing)

**POST** `/api/v1/providers/validate/batch`

Validate multiple providers in parallel.

```bash
curl -X POST "http://localhost:8000/api/v1/providers/validate/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "providers": [
      {
        "establishment_name": "AAROGYA HOSPITAL",
        "certificate_number": "BDR00172ALHL2"
      },
      {
        "establishment_name": "TEST CLINIC",
        "phone": "9876543210"
      }
    ],
    "max_concurrent": 5
  }'
```

**Response:**
```json
{
  "request_id": "batch_xyz789",
  "timestamp": "2026-01-04T19:30:00Z",
  "total_providers": 2,
  "completed": 2,
  "failed": 0,
  "execution_time_ms": 1500,
  "results": [ ... ],
  "errors": []
}
```

### Health & Monitoring

#### 1. Basic Health Check

**GET** `/api/v1/health`

```bash
curl "http://localhost:8000/api/v1/health"
```

#### 2. Services Health

**GET** `/api/v1/health/services`

Check health of database and cache.

```bash
curl "http://localhost:8000/api/v1/health/services"
```

#### 3. Readiness Probe (Kubernetes)

**GET** `/api/v1/health/ready`

#### 4. Liveness Probe (Kubernetes)

**GET** `/api/v1/health/live`

### Admin & Statistics

#### 1. System Statistics

**GET** `/api/v1/admin/stats`

Get comprehensive system statistics.

```bash
curl "http://localhost:8000/api/v1/admin/stats"
```

**Response:**
```json
{
  "total_establishments": 1000,
  "total_staff": 4483,
  "total_districts": 31,
  "total_validations": 1250,
  "auto_approved": 850,
  "auto_rejected": 200,
  "manual_review": 200,
  "avg_response_time_ms": 450.5,
  "cache_hit_rate": 0.65,
  "timestamp": "2026-01-04T19:30:00Z"
}
```

#### 2. Search Establishments

**GET** `/api/v1/admin/search/establishments`

Search for healthcare establishments.

```bash
curl "http://localhost:8000/api/v1/admin/search/establishments?query=AAROGYA&limit=10"
```

#### 3. Search Staff

**GET** `/api/v1/admin/search/staff`

Search for healthcare staff by registration number.

```bash
curl "http://localhost:8000/api/v1/admin/search/staff?registration_number=REG12345"
```

#### 4. Manual Review Queue (Placeholder)

**GET** `/api/v1/admin/manual-review`

Get providers pending manual review.

```bash
curl "http://localhost:8000/api/v1/admin/manual-review?skip=0&limit=50"
```

## Request/Response Headers

### Request Headers

- `X-Request-ID` (optional): Custom request ID for tracking
- `X-API-Key` (optional): API key for authentication (placeholder)

### Response Headers

- `X-Request-ID`: Request ID for tracking
- `X-Process-Time-MS`: Processing time in milliseconds

## Error Handling

All errors follow this format:

```json
{
  "error": "Error type",
  "detail": "Detailed error message",
  "request_id": "req_abc123",
  "timestamp": "2026-01-04T19:30:00Z"
}
```

**Status Codes:**
- `200`: Success
- `400`: Bad request (validation error)
- `401`: Unauthorized
- `403`: Forbidden
- `404`: Not found
- `429`: Rate limit exceeded
- `500`: Internal server error
- `501`: Not implemented

## Performance

### Fast Validator
- **Speed**: 2,306 validations/second
- **API Calls**: 0 (fully deterministic)
- **Cost**: $0 per validation
- **Use Case**: Real-time form validation, bulk checks

### Data Validator
- **Speed**: ~2 validations/second
- **API Calls**: 1 (AI synthesis only)
- **Cost**: ~$0.001 per validation
- **Use Case**: Complete provider onboarding, audits

## Architecture

### Deterministic-First Design

- **90% of operations**: Direct database queries (no AI)
- **10% of operations**: AI synthesis for complex decisions

### Dual Path System

1. **Simple Path** (Fast Validator):
   - Complete KPME data available
   - Direct certificate lookup
   - No AI calls
   - ~0.4ms response time

2. **Complex Path** (Data Validator):
   - Incomplete or ambiguous data
   - Multi-source validation
   - AI synthesis for final decision
   - ~500ms response time

### Cache Strategy

- **Primary**: Redis (if available)
- **Fallback**: In-memory LRU cache
- **TTL**: 24 hours for KPME lookups
- **Namespacing**: `kpme_fast:`, `kpme_data:`

## Configuration

### Environment Variables

Required:
```bash
MISTRAL_API_KEY=your-mistral-api-key  # For AI synthesis
```

Optional:
```bash
REDIS_URL=redis://localhost:6379/0
REDIS_PASSWORD=your-redis-password
CACHE_ENABLED=true
```

### Production Deployment

#### Using Uvicorn

```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 4
```

#### Using Gunicorn + Uvicorn

```bash
gunicorn src.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120 \
  --access-logfile - \
  --error-logfile -
```

#### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Testing

### Run All Tests

```bash
# Start the API
python src/main.py

# In another terminal, run tests
python test_api.py
```

### Example Test Output

```
================================================================================
KPME Provider Validation API - Test Suite
Testing: http://localhost:8000
Time: 2026-01-04T19:30:00
================================================================================

TEST 1: Root Endpoint
Status: 200
✅ Root endpoint working

TEST 2: Health Check
Status: 200
✅ Health check working

...

✅ ALL TESTS PASSED!
================================================================================
```

## Frontend Integration

### React Example

```javascript
async function validateProvider(data) {
  const response = await fetch('http://localhost:8000/api/v1/providers/validate', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data)
  });

  return await response.json();
}

// Usage
const result = await validateProvider({
  establishment_name: "AAROGYA HOSPITAL",
  certificate_number: "BDR00172ALHL2",
  phone: "9448452147"
});

console.log(`Decision: ${result.decision}`);
console.log(`Confidence: ${result.final_confidence}`);
```

### Vue Example

```javascript
export default {
  methods: {
    async validateProvider() {
      try {
        const response = await this.$http.post('/api/v1/providers/validate', {
          establishment_name: this.establishmentName,
          certificate_number: this.certificateNumber,
          phone: this.phone
        });

        this.validationResult = response.data;
      } catch (error) {
        console.error('Validation failed:', error);
      }
    }
  }
}
```

## Rate Limiting (TODO)

Currently no rate limiting is implemented. In production:

1. Implement per-API-key rate limiting
2. Use Redis for distributed rate limiting
3. Suggested limits:
   - Fast validator: 100 requests/minute
   - Full validator: 20 requests/minute
   - Batch validator: 5 requests/minute

## Authentication (TODO)

Currently using placeholder authentication. In production:

1. Implement API key validation
2. Use JWT tokens for user authentication
3. Add role-based access control (RBAC)
4. Secure admin endpoints

## Monitoring (TODO)

Integrate with monitoring tools:

- **Prometheus**: Metrics collection
- **Grafana**: Dashboards
- **Sentry**: Error tracking
- **ELK Stack**: Log aggregation

## Support

For issues or questions:
- Check the API documentation: http://localhost:8000/docs
- Review system logs
- Contact the development team

## License

Proprietary - KPME Provider Validation System
