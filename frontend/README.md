# KPME Provider Validation System - Frontend

Beautiful, responsive React frontend for the KPME Karnataka Healthcare Provider Validation System.

## Features

### 1. Full Provider Validation
- Complete validation workflow with AI-powered synthesis
- Multi-field form for comprehensive provider data
- Real-time validation results with confidence scoring
- Decision reasoning and detailed analytics
- Test data button for quick testing

### 2. Fast Certificate Validation
- Ultra-fast KPME certificate validation (2,306 validations/second)
- Deterministic validation (0 AI calls)
- Cache hit/miss indicators
- Sub-second response times

### 3. Search Functionality
- Search KPME database by establishment name
- Real-time search results
- Detailed establishment information display
- Certificate, district, and contact information

### 4. System Health Monitoring
- Real-time API health status
- Database health check (1,000 establishments)
- Cache status (Redis/Memory)
- Service uptime tracking

### 5. System Statistics Dashboard
- Total establishments, staff, districts
- Validation metrics (auto-approved, rejected, manual review)
- Beautiful stat cards with gradient backgrounds
- Real-time refresh capability

## Technology Stack

- **React 18** - UI framework (loaded from CDN)
- **Vanilla JavaScript** - No build process required
- **CSS3** - Modern, responsive styling
- **Fetch API** - HTTP client for API calls

## Quick Start

### Method 1: Using the Start Script (Recommended)

```bash
# Start both backend and frontend
python start_app.py
```

This will:
- Start FastAPI backend on http://localhost:8000
- Start frontend on http://localhost:3000
- Automatically open the browser

### Method 2: Manual Start

**Terminal 1 - Backend:**
```bash
python src/main.py
```

**Terminal 2 - Frontend:**
```bash
cd frontend
python -m http.server 3000
```

**Open Browser:**
```
http://localhost:3000
```

## Testing

### Automated Integration Tests

```bash
python test_frontend_integration.py
```

This runs 8 comprehensive tests:
1. Backend health check
2. Frontend accessibility
3. Fast certificate validation
4. Full provider validation
5. Services health check
6. System statistics
7. Establishment search
8. CORS configuration

### Manual Testing

**Test with Sample Data:**
1. Go to "Full Validation" tab
2. Click "Load Test Data"
3. Click "Validate Provider"
4. View detailed results

**Fast Validation:**
1. Go to "Fast Validation" tab
2. Click "Load Test Data"
3. Click "Fast Validate"
4. See instant results

**Search:**
1. Go to "Search" tab
2. Type "AAROGYA" or "HOSPITAL"
3. View search results

## API Integration

The frontend connects to the FastAPI backend at `http://localhost:8000`.

### Endpoints Used

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/providers/validate` | POST | Full provider validation |
| `/api/v1/providers/validate/fast` | POST | Fast certificate validation |
| `/api/v1/health` | GET | API health check |
| `/api/v1/health/services` | GET | Services health |
| `/api/v1/admin/stats` | GET | System statistics |
| `/api/v1/admin/search/establishments` | GET | Search establishments |

### CORS Configuration

The frontend is configured to work with the backend's CORS settings:
- Allowed origins: `*` (development) - should be restricted in production
- Allowed methods: All
- Exposed headers: `X-Request-ID`, `X-Process-Time-MS`

## UI Features

### Responsive Design
- Mobile-friendly layout
- Adaptive grid system
- Touch-optimized controls

### Visual Feedback
- Loading spinners during API calls
- Success/error message displays
- Confidence bar visualizations
- Color-coded status badges

### Color Scheme
- Primary: Purple gradient (#667eea → #764ba2)
- Success: Green (#28a745)
- Error: Red (#dc3545)
- Warning: Yellow (#ffc107)

### Status Badges
- **Auto Approved**: Green background
- **Auto Rejected**: Red background
- **Manual Review**: Yellow background

## File Structure

```
frontend/
├── index.html          # Main HTML file with React CDN links
├── styles.css          # Complete CSS styling (1,000+ lines)
├── app.js              # React application (1,500+ lines)
└── README.md           # This file
```

## Key Components

### App
Main application component with tab navigation.

### FullValidation
Complete provider validation form with multi-field support.

### FastValidation
Quick certificate validation with minimal input.

### SystemHealth
Health monitoring dashboard for API and services.

### SystemStats
Statistical dashboard with metrics visualization.

### Search
Establishment search with results display.

### ValidationResult
Detailed validation result display with JSON viewer.

### FastValidationResult
Fast validation result display with performance metrics.

## Customization

### Changing API URL

Edit `app.js`:
```javascript
const API_BASE_URL = 'http://localhost:8000';  // Change this
```

### Styling

Edit `styles.css` to customize:
- Colors (gradients, backgrounds)
- Fonts and typography
- Spacing and layout
- Responsive breakpoints

### Adding New Tabs

1. Add tab button in `App` component
2. Create new component
3. Add conditional rendering in `App`

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- Mobile browsers

## Performance

- **Initial Load**: < 2 seconds
- **Fast Validation**: < 1 second
- **Full Validation**: 2-4 seconds (depending on data)
- **Search**: < 1 second

## Known Issues

- Redis cache shows "unhealthy" if Redis is not installed (falls back to memory cache)
- Some validation metrics show 0 (placeholders for future implementation)

## Future Enhancements

- [ ] Batch validation UI
- [ ] Manual review queue interface
- [ ] Real-time validation metrics tracking
- [ ] Export results to CSV/PDF
- [ ] Dark mode toggle
- [ ] Validation history
- [ ] User authentication

## Troubleshooting

### Frontend not loading
- Check if Python HTTP server is running on port 3000
- Verify `frontend` directory contains all files

### API calls failing
- Ensure backend is running on port 8000
- Check CORS configuration
- Verify API URL in `app.js`

### Validation not working
- Check browser console for errors
- Verify Mistral API key is configured in backend
- Ensure database is loaded (1,000 establishments)

## Production Deployment

For production:

1. **Build with a bundler** (optional but recommended):
   ```bash
   npm install
   npm run build
   ```

2. **Serve with nginx/Apache**:
   ```nginx
   server {
       listen 80;
       root /path/to/frontend;
       index index.html;
   }
   ```

3. **Update API URL**:
   - Change `API_BASE_URL` to production backend URL
   - Update CORS settings in backend

4. **Enable HTTPS**:
   - Use Let's Encrypt or similar
   - Update frontend to use HTTPS URLs

## Support

For issues or questions:
- Check backend API documentation: http://localhost:8000/docs
- Review integration tests: `python test_frontend_integration.py`
- Check browser console for errors

## License

Proprietary - KPME Provider Validation System
