const { useState, useEffect } = React;

const API_BASE_URL = 'http://localhost:8000';

// API service with error handling
const api = {
    async request(endpoint, options = {}) {
        try {
            const response = await fetch(`${API_BASE_URL}${endpoint}`, {
                ...options,
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers,
                },
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || `HTTP ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('API Error:', error);
            throw error;
        }
    },

    async validateFull(data) {
        return this.request('/api/v1/providers/validate', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    },

    async validateFast(data) {
        return this.request('/api/v1/providers/validate/fast', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    },

    async getHealth() {
        return this.request('/api/v1/health');
    },

    async getServicesHealth() {
        return this.request('/api/v1/health/services');
    },

    async getStats() {
        return this.request('/api/v1/admin/stats');
    },

    async searchEstablishments(query, limit = 10) {
        const params = new URLSearchParams({ query, limit: limit.toString() });
        return this.request(`/api/v1/admin/search/establishments?${params}`);
    },
};

// OCR Scanner Component
function OCRScanner({ onDataExtracted }) {
    const [file, setFile] = useState(null);
    const [preview, setPreview] = useState(null);
    const [processing, setProcessing] = useState(false);
    const [progress, setProgress] = useState(0);
    const [extractedText, setExtractedText] = useState('');
    const [structuredData, setStructuredData] = useState(null);
    const [error, setError] = useState('');
    const [isDragging, setIsDragging] = useState(false);

    const handleFileSelect = (selectedFile) => {
        if (!selectedFile) return;

        if (!selectedFile.type.startsWith('image/')) {
            setError('Please select an image file');
            return;
        }

        setFile(selectedFile);
        setError('');
        setExtractedText('');
        setStructuredData(null);

        const reader = new FileReader();
        reader.onload = (e) => setPreview(e.target.result);
        reader.readAsDataURL(selectedFile);
    };

    const handleDrop = (e) => {
        e.preventDefault();
        setIsDragging(false);
        const droppedFile = e.dataTransfer.files[0];
        handleFileSelect(droppedFile);
    };

    const handleDragOver = (e) => {
        e.preventDefault();
        setIsDragging(true);
    };

    const handleDragLeave = () => {
        setIsDragging(false);
    };

    const parseExtractedText = (text) => {
        const data = {
            establishment_name: '',
            certificate_number: '',
            phone: '',
            email: '',
            address: '',
            district: '',
        };

        // Extract certificate number (e.g., BDR00172ALHL2)
        const certMatch = text.match(/[A-Z]{2,4}\d{4,10}[A-Z]{0,4}/);
        if (certMatch) {
            data.certificate_number = certMatch[0];
        }

        // Extract Indian phone number
        const phoneMatch = text.match(/(?:\+91|0)?[\s-]?([6-9]\d{9})/);
        if (phoneMatch) {
            data.phone = phoneMatch[1];
        }

        // Extract email
        const emailMatch = text.match(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/i);
        if (emailMatch) {
            data.email = emailMatch[0];
        }

        // Extract hospital/clinic name (look for common patterns)
        const namePatterns = [
            /(?:hospital|clinic|medical center|healthcare|nursing home)[:\s]+([^\n]+)/i,
            /name[:\s]+([^\n]+)/i,
            /establishment[:\s]+([^\n]+)/i,
        ];

        for (const pattern of namePatterns) {
            const nameMatch = text.match(pattern);
            if (nameMatch && nameMatch[1].trim().length > 3) {
                data.establishment_name = nameMatch[1].trim();
                break;
            }
        }

        // Extract district (Karnataka districts)
        const districts = [
            'Bangalore Urban', 'Bangalore Rural', 'Mysore', 'Mangalore', 'Hubli', 'Dharwad',
            'Belgaum', 'Gulbarga', 'Bellary', 'Tumkur', 'Shimoga', 'Davangere', 'Bijapur',
            'Raichur', 'Udupi', 'Hassan', 'Chitradurga', 'Kolar', 'Mandya', 'Chikmagalur'
        ];

        for (const district of districts) {
            if (text.includes(district)) {
                data.district = district;
                break;
            }
        }

        // Extract address (look for common address indicators)
        const addressMatch = text.match(/(?:address|location)[:\s]+([^\n]+(?:\n[^\n]+){0,2})/i);
        if (addressMatch) {
            data.address = addressMatch[1].trim().replace(/\n/g, ', ');
        }

        return data;
    };

    const performOCR = async () => {
        if (!file) return;

        setProcessing(true);
        setProgress(0);
        setError('');

        try {
            // Create FormData for file upload
            const formData = new FormData();
            formData.append('file', file);

            // Call backend OCR API (uses Google Cloud Vision or OCR.space)
            setProgress(30);
            const response = await fetch(`${API_BASE_URL}/api/v1/ocr/extract`, {
                method: 'POST',
                body: formData,
            });

            setProgress(60);

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'OCR processing failed');
            }

            const data = await response.json();
            setProgress(90);

            // Extract results from backend response
            setExtractedText(data.extracted_text);
            setStructuredData(data.structured_data);

            setProgress(100);

            // Auto-fill parent form if callback provided
            if (onDataExtracted) {
                onDataExtracted(data.structured_data);
            }
        } catch (err) {
            setError('OCR processing failed: ' + err.message);
            console.error('OCR error:', err);
        } finally {
            setProcessing(false);
        }
    };

    return (
        <div className="ocr-scanner">
            <h3 className="section-title">📄 Document Scanner (OCR)</h3>
            <p className="section-subtitle">Upload a prescription or medical record image to extract information</p>

            <div
                className={`ocr-upload-zone ${isDragging ? 'dragging' : ''}`}
                onDrop={handleDrop}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onClick={() => document.getElementById('file-input').click()}
            >
                {preview ? (
                    <div className="ocr-preview">
                        <img src={preview} alt="Preview" />
                    </div>
                ) : (
                    <>
                        <div className="ocr-upload-icon">📸</div>
                        <p className="ocr-upload-text">Drag & drop an image here or click to select</p>
                        <p className="ocr-upload-hint">Supports JPG, PNG, PDF (image-based)</p>
                    </>
                )}
                <input
                    id="file-input"
                    type="file"
                    accept="image/*"
                    onChange={(e) => handleFileSelect(e.target.files[0])}
                    style={{ display: 'none' }}
                />
            </div>

            {file && !processing && (
                <button className="btn btn-primary" onClick={performOCR}>
                    🔍 Extract Text from Image
                </button>
            )}

            {processing && (
                <div className="ocr-progress">
                    <div className="progress-bar">
                        <div className="progress-fill" style={{ width: `${progress}%` }}></div>
                    </div>
                    <p className="progress-text">Processing... {progress}%</p>
                </div>
            )}

            {error && <div className="error-message">{error}</div>}

            {extractedText && (
                <div className="ocr-results">
                    <h4 className="result-title">Extracted Text</h4>
                    <div className="extracted-text-box">{extractedText}</div>
                </div>
            )}

            {structuredData && (
                <div className="ocr-structured-data">
                    <h4 className="result-title">Parsed Information</h4>
                    <div className="structured-data-grid">
                        {Object.entries(structuredData).map(([key, value]) => (
                            value && (
                                <div key={key} className="data-field">
                                    <span className="data-label">{key.replace(/_/g, ' ')}:</span>
                                    <span className="data-value">{value}</span>
                                </div>
                            )
                        ))}
                    </div>
                    <p className="ocr-hint">✨ Data has been auto-filled in the validation form below</p>
                </div>
            )}
        </div>
    );
}

// Full Validation Component
function FullValidation() {
    const [formData, setFormData] = useState({
        establishment_name: '',
        certificate_number: '',
        phone: '',
        email: '',
        district: '',
        address: '',
    });
    const [result, setResult] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [showOCR, setShowOCR] = useState(false);

    const handleOCRData = (data) => {
        setFormData(prev => ({
            ...prev,
            ...data,
        }));
        setShowOCR(false);
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError('');
        setResult(null);

        try {
            const data = await api.validateFull(formData);
            setResult(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const loadNonWorkingTestData = () => {
        setFormData({
            establishment_name: 'AAROGYA HOSPITAL',
            certificate_number: 'BDR00172ALHL2',
            phone: '9448452147',
            email: 'aarogya@example.com',
            district: 'Bangalore Urban',
            address: '123 Main Street, Bangalore',
        });
    };

    const loadValidTestData = () => {
        setFormData({
            establishment_name: 'DR HABIB KHAN',
            certificate_number: 'DKA00409ALCOC',
            district: 'MANGALORE',
            category: 'Clinic/Polyclinic Only Consultation',
            system_of_medicine: 'Allopathy',
            address: '1ST FLOOR, SHUBHA KRISHNA BUILDING, HIGHLANDS, MANGALORE',
        });
    };

    return (
        <div className="validation-container">
            <div className="form-section">
                <div className="section-header">
                    <h3 className="section-title">🏥 Full Provider Validation</h3>
                    <button
                        className="btn btn-secondary btn-sm"
                        onClick={() => setShowOCR(!showOCR)}
                    >
                        {showOCR ? '✕ Close Scanner' : '📸 Scan Document'}
                    </button>
                </div>

                {showOCR && <OCRScanner onDataExtracted={handleOCRData} />}

                <form onSubmit={handleSubmit} className="validation-form">
                    <div className="form-row">
                        <div className="form-group">
                            <label className="form-label">Establishment Name *</label>
                            <input
                                type="text"
                                className="form-input"
                                value={formData.establishment_name}
                                onChange={(e) => setFormData({ ...formData, establishment_name: e.target.value })}
                                required
                            />
                        </div>

                        <div className="form-group">
                            <label className="form-label">Certificate Number *</label>
                            <input
                                type="text"
                                className="form-input"
                                value={formData.certificate_number}
                                onChange={(e) => setFormData({ ...formData, certificate_number: e.target.value })}
                                required
                            />
                        </div>
                    </div>

                    <div className="form-row">
                        <div className="form-group">
                            <label className="form-label">Phone Number *</label>
                            <input
                                type="tel"
                                className="form-input"
                                value={formData.phone}
                                onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                                required
                            />
                        </div>

                        <div className="form-group">
                            <label className="form-label">Email</label>
                            <input
                                type="email"
                                className="form-input"
                                value={formData.email}
                                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                            />
                        </div>
                    </div>

                    <div className="form-row">
                        <div className="form-group">
                            <label className="form-label">District *</label>
                            <input
                                type="text"
                                className="form-input"
                                value={formData.district}
                                onChange={(e) => setFormData({ ...formData, district: e.target.value })}
                                required
                            />
                        </div>

                        <div className="form-group">
                            <label className="form-label">Address</label>
                            <input
                                type="text"
                                className="form-input"
                                value={formData.address}
                                onChange={(e) => setFormData({ ...formData, address: e.target.value })}
                            />
                        </div>
                    </div>

                    <div className="form-actions">
                        <button type="submit" className="btn btn-primary" disabled={loading}>
                            {loading ? '⏳ Validating...' : '✓ Validate Provider'}
                        </button>
                        <button type="button" className="btn btn-secondary" onClick={loadValidTestData}>
                            ✅ Load Valid Test Data
                        </button>
                        <button type="button" className="btn btn-secondary" onClick={loadNonWorkingTestData}>
                            📋 Non-Working Test Data
                        </button>
                    </div>
                </form>

                {error && <div className="error-message">{error}</div>}
            </div>

            {result && <ValidationResult result={result} />}
        </div>
    );
}

// Validation Result Component
function ValidationResult({ result }) {
    const [showJSON, setShowJSON] = useState(false);

    const getDecisionBadgeClass = (decision) => {
        if (decision === 'auto_approved') return 'badge-success';
        if (decision === 'auto_rejected') return 'badge-error';
        return 'badge-warning';
    };

    const getConfidenceBarClass = (confidence) => {
        if (confidence >= 0.8) return 'confidence-high';
        if (confidence >= 0.5) return 'confidence-medium';
        return 'confidence-low';
    };

    return (
        <div className="result-section">
            <h3 className="section-title">📊 Validation Results</h3>

            <div className="result-card">
                <div className="result-header">
                    <span className={`decision-badge ${getDecisionBadgeClass(result.decision)}`}>
                        {result.decision?.replace(/_/g, ' ').toUpperCase()}
                    </span>
                    <span className="confidence-score">
                        Confidence: {(result.final_confidence * 100).toFixed(1)}%
                        <span className="confidence-level">({result.confidence_level})</span>
                    </span>
                </div>

                <div className="confidence-bar-container">
                    <div className={`confidence-bar ${getConfidenceBarClass(result.final_confidence)}`}>
                        <div
                            className="confidence-fill"
                            style={{ width: `${result.final_confidence * 100}%` }}
                        ></div>
                    </div>
                </div>

                <div className="result-details">
                    <div className="detail-item">
                        <span className="detail-label">Validation Path:</span>
                        <span className="detail-value">{result.validation_path}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Execution Time:</span>
                        <span className="detail-value">{result.execution_time_ms}ms</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Request ID:</span>
                        <span className="detail-value detail-id">{result.request_id}</span>
                    </div>
                </div>

                {result.reasoning && result.reasoning.length > 0 && (
                    <div className="reasoning-section">
                        <h4 className="reasoning-title">💡 Reasoning</h4>
                        <ul className="reasoning-list">
                            {result.reasoning.map((reason, idx) => (
                                <li key={idx} className="reasoning-item">{reason}</li>
                            ))}
                        </ul>
                    </div>
                )}

                <div className="json-toggle">
                    <button
                        className="btn btn-secondary btn-sm"
                        onClick={() => setShowJSON(!showJSON)}
                    >
                        {showJSON ? 'Hide' : 'Show'} JSON
                    </button>
                </div>

                {showJSON && (
                    <div className="json-viewer">
                        <pre>{JSON.stringify(result, null, 2)}</pre>
                    </div>
                )}
            </div>
        </div>
    );
}

// Fast Validation Component
function FastValidation() {
    const [formData, setFormData] = useState({
        certificate_number: '',
        provider_name: '',
        phone: '',
    });
    const [result, setResult] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError('');
        setResult(null);

        try {
            const data = await api.validateFast(formData);
            setResult(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const loadNonWorkingTestData = () => {
        setFormData({
            certificate_number: 'BDR00172ALHL2',
            provider_name: 'AAROGYA HOSPITAL',
            phone: '9448452147',
        });
    };

    const loadValidTestData = () => {
        setFormData({
            certificate_number: 'DKA00409ALCOC',
            provider_name: 'DR HABIB KHAN',
            phone: '',
        });
    };

    return (
        <div className="validation-container">
            <div className="form-section">
                <h3 className="section-title">⚡ Fast Certificate Validation</h3>
                <p className="section-subtitle">Ultra-fast KPME validation (2,306 validations/second)</p>

                <form onSubmit={handleSubmit} className="validation-form">
                    <div className="form-group">
                        <label className="form-label">Certificate Number *</label>
                        <input
                            type="text"
                            className="form-input"
                            value={formData.certificate_number}
                            onChange={(e) => setFormData({ ...formData, certificate_number: e.target.value })}
                            required
                        />
                    </div>

                    <div className="form-group">
                        <label className="form-label">Provider Name</label>
                        <input
                            type="text"
                            className="form-input"
                            value={formData.provider_name}
                            onChange={(e) => setFormData({ ...formData, provider_name: e.target.value })}
                        />
                    </div>

                    <div className="form-group">
                        <label className="form-label">Phone Number</label>
                        <input
                            type="tel"
                            className="form-input"
                            value={formData.phone}
                            onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                        />
                    </div>

                    <div className="form-actions">
                        <button type="submit" className="btn btn-primary" disabled={loading}>
                            {loading ? '⏳ Validating...' : '⚡ Fast Validate'}
                        </button>
                        <button type="button" className="btn btn-secondary" onClick={loadValidTestData}>
                            ✅ Load Valid Test Data
                        </button>
                        <button type="button" className="btn btn-secondary" onClick={loadNonWorkingTestData}>
                            📋 Non-Working Test Data
                        </button>
                    </div>
                </form>

                {error && <div className="error-message">{error}</div>}
            </div>

            {result && <FastValidationResult result={result} />}
        </div>
    );
}

// Fast Validation Result Component
function FastValidationResult({ result }) {
    const [showJSON, setShowJSON] = useState(false);

    return (
        <div className="result-section">
            <h3 className="section-title">⚡ Fast Validation Results</h3>

            <div className="result-card">
                <div className="result-header">
                    <span className={`decision-badge ${result.is_valid ? 'badge-success' : 'badge-error'}`}>
                        {result.is_valid ? '✓ VALID' : '✗ INVALID'}
                    </span>
                    <span className="confidence-score">
                        Confidence: {(result.confidence * 100).toFixed(1)}%
                    </span>
                </div>

                <div className="result-details">
                    <div className="detail-item">
                        <span className="detail-label">Certificate:</span>
                        <span className="detail-value">{result.certificate_number || 'N/A'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Provider:</span>
                        <span className="detail-value">{result.establishment_name || 'N/A'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Cache Hit:</span>
                        <span className="detail-value">{result.cache_hit ? '✓ Yes' : '✗ No'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Execution Time:</span>
                        <span className="detail-value">{result.execution_time_ms}ms</span>
                    </div>
                </div>

                {/* AI Explainability Section */}
                {result.confidence > 0 && (
                    <div className="reasoning-section">
                        <h4 className="reasoning-title">🤖 AI Confidence Breakdown</h4>
                        <div className="explainability-box">
                            <div className="explanation-item">
                                <span className="explain-label">✓ KPME Database Validation:</span>
                                <span className="explain-score">
                                    {result.is_expired ? '50%' : '90%'} confidence
                                </span>
                            </div>
                            <div className="explanation-details">
                                {result.is_valid && !result.is_expired && (
                                    <>
                                        <p>✓ Certificate found in KPME Karnataka database</p>
                                        <p>✓ Certificate is currently valid (not expired)</p>
                                        <p>✓ Establishment details match: <strong>{result.establishment_name}</strong></p>
                                    </>
                                )}
                                {result.is_valid && result.is_expired && (
                                    <>
                                        <p>⚠️ Certificate found in KPME database</p>
                                        <p>⚠️ Certificate has expired - reduced confidence to 50%</p>
                                    </>
                                )}
                                {!result.is_valid && (
                                    <>
                                        <p>✗ Certificate not found in KPME database</p>
                                        <p>✗ No matching establishment records</p>
                                    </>
                                )}
                            </div>
                            <div className="validation-source">
                                <small>Validation Source: KPME Karnataka Database (Direct Lookup)</small>
                            </div>
                        </div>
                    </div>
                )}

                {result.message && (
                    <div className="message-box">
                        <p>{result.message}</p>
                    </div>
                )}

                <div className="json-toggle">
                    <button
                        className="btn btn-secondary btn-sm"
                        onClick={() => setShowJSON(!showJSON)}
                    >
                        {showJSON ? 'Hide' : 'Show'} JSON
                    </button>
                </div>

                {showJSON && (
                    <div className="json-viewer">
                        <pre>{JSON.stringify(result, null, 2)}</pre>
                    </div>
                )}
            </div>
        </div>
    );
}

// System Health Component
function SystemHealth() {
    const [health, setHealth] = useState(null);
    const [servicesHealth, setServicesHealth] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    const fetchHealth = async () => {
        setLoading(true);
        setError('');

        try {
            const [healthData, servicesData] = await Promise.all([
                api.getHealth(),
                api.getServicesHealth(),
            ]);
            setHealth(healthData);
            setServicesHealth(servicesData);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHealth();
    }, []);

    const getStatusBadgeClass = (status) => {
        if (status === 'healthy' || status === 'ok') return 'badge-success';
        if (status === 'degraded') return 'badge-warning';
        return 'badge-error';
    };

    return (
        <div className="health-container">
            <div className="section-header">
                <h3 className="section-title">❤️ System Health</h3>
                <button className="btn btn-secondary btn-sm" onClick={fetchHealth} disabled={loading}>
                    {loading ? '⏳' : '🔄'} Refresh
                </button>
            </div>

            {error && <div className="error-message">{error}</div>}

            {health && (
                <div className="health-card">
                    <div className="health-header">
                        <h4>API Health</h4>
                        <span className={`status-badge ${getStatusBadgeClass(health.status)}`}>
                            {health.status?.toUpperCase()}
                        </span>
                    </div>
                    <div className="health-details">
                        <div className="detail-item">
                            <span className="detail-label">Version:</span>
                            <span className="detail-value">{health.version}</span>
                        </div>
                        <div className="detail-item">
                            <span className="detail-label">Uptime:</span>
                            <span className="detail-value">{health.uptime_seconds?.toFixed(2)}s</span>
                        </div>
                    </div>
                </div>
            )}

            {servicesHealth && (
                <>
                    <div className="health-card">
                        <div className="health-header">
                            <h4>Database</h4>
                            <span className={`status-badge ${getStatusBadgeClass(servicesHealth.database?.status)}`}>
                                {servicesHealth.database?.status?.toUpperCase()}
                            </span>
                        </div>
                        <div className="health-details">
                            <div className="detail-item">
                                <span className="detail-label">Establishments:</span>
                                <span className="detail-value">{servicesHealth.database?.total_establishments || 0}</span>
                            </div>
                            <div className="detail-item">
                                <span className="detail-label">Staff:</span>
                                <span className="detail-value">{servicesHealth.database?.total_staff || 0}</span>
                            </div>
                        </div>
                    </div>

                    <div className="health-card">
                        <div className="health-header">
                            <h4>Cache</h4>
                            <span className={`status-badge ${getStatusBadgeClass(servicesHealth.cache?.status)}`}>
                                {servicesHealth.cache?.status?.toUpperCase()}
                            </span>
                        </div>
                        <div className="health-details">
                            <div className="detail-item">
                                <span className="detail-label">Type:</span>
                                <span className="detail-value">{servicesHealth.cache?.type || 'Unknown'}</span>
                            </div>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}

// System Stats Component
function SystemStats() {
    const [stats, setStats] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    const fetchStats = async () => {
        setLoading(true);
        setError('');

        try {
            const data = await api.getStats();
            setStats(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchStats();
    }, []);

    return (
        <div className="stats-container">
            <div className="section-header">
                <h3 className="section-title">📊 System Statistics</h3>
                <button className="btn btn-secondary btn-sm" onClick={fetchStats} disabled={loading}>
                    {loading ? '⏳' : '🔄'} Refresh
                </button>
            </div>

            {error && <div className="error-message">{error}</div>}

            {stats && (
                <div className="stats-grid">
                    <div className="stat-card stat-card-primary">
                        <div className="stat-icon">🏥</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.total_establishments || 0}</div>
                            <div className="stat-label">Total Establishments</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-secondary">
                        <div className="stat-icon">👨‍⚕️</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.total_staff || 0}</div>
                            <div className="stat-label">Total Staff</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-accent">
                        <div className="stat-icon">📍</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.total_districts || 0}</div>
                            <div className="stat-label">Total Districts</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-success">
                        <div className="stat-icon">✓</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.total_validations || 0}</div>
                            <div className="stat-label">Total Validations</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-success">
                        <div className="stat-icon">✓</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.auto_approved || 0}</div>
                            <div className="stat-label">Auto Approved</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-error">
                        <div className="stat-icon">✗</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.auto_rejected || 0}</div>
                            <div className="stat-label">Auto Rejected</div>
                        </div>
                    </div>

                    <div className="stat-card stat-card-warning">
                        <div className="stat-icon">⚠</div>
                        <div className="stat-content">
                            <div className="stat-value">{stats.manual_review || 0}</div>
                            <div className="stat-label">Manual Review</div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

// Search Component
function Search() {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    const handleSearch = async (e) => {
        e.preventDefault();
        if (!query.trim()) return;

        setLoading(true);
        setError('');

        try {
            const data = await api.searchEstablishments(query, 100);
            setResults(data);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="search-container">
            <h3 className="section-title">🔍 Search Establishments</h3>
            <p className="section-subtitle">Search the KPME database by establishment name</p>

            <form onSubmit={handleSearch} className="search-form">
                <input
                    type="text"
                    className="search-input"
                    placeholder="Enter establishment name..."
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                />
                <button type="submit" className="btn btn-primary" disabled={loading}>
                    {loading ? '⏳ Searching...' : '🔍 Search'}
                </button>
            </form>

            {error && <div className="error-message">{error}</div>}

            {results && (
                <div className="search-results">
                    <div className="results-header">
                        <h4>Search Results</h4>
                        <span className="results-count">{results.total_results} results found</span>
                    </div>

                    {results.results && results.results.length > 0 ? (
                        <div className="results-list">
                            {results.results.map((item, idx) => (
                                <div key={idx} className="result-item">
                                    <h5 className="result-name">{item.establishment_name || item.name}</h5>
                                    <div className="result-details">
                                        {item.certificate_number && (
                                            <div className="result-detail">
                                                <span className="result-label">Certificate:</span>
                                                <span className="result-value">{item.certificate_number}</span>
                                            </div>
                                        )}
                                        {item.district && (
                                            <div className="result-detail">
                                                <span className="result-label">District:</span>
                                                <span className="result-value">{item.district}</span>
                                            </div>
                                        )}
                                        {item.phone && (
                                            <div className="result-detail">
                                                <span className="result-label">Phone:</span>
                                                <span className="result-value">{item.phone}</span>
                                            </div>
                                        )}
                                        {item.email && (
                                            <div className="result-detail">
                                                <span className="result-label">Email:</span>
                                                <span className="result-value">{item.email}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="no-results">No results found</div>
                    )}
                </div>
            )}
        </div>
    );
}

// Main App Component
function App() {
    const [activeTab, setActiveTab] = useState('full');

    return (
        <div className="app">
            <header className="header">
                <div className="header-content">
                    <div className="logo">
                        <div className="logo-icon">🏥</div>
                        <div className="logo-text">
                            <h1>KPME Clinical Validation System</h1>
                            <p className="tagline">Karnataka Healthcare Provider Verification</p>
                        </div>
                    </div>
                </div>
            </header>

            <nav className="tabs">
                <button
                    className={`tab ${activeTab === 'full' ? 'active' : ''}`}
                    onClick={() => setActiveTab('full')}
                >
                    🏥 Full Validation
                </button>
                <button
                    className={`tab ${activeTab === 'fast' ? 'active' : ''}`}
                    onClick={() => setActiveTab('fast')}
                >
                    ⚡ Fast Validation
                </button>
                <button
                    className={`tab ${activeTab === 'search' ? 'active' : ''}`}
                    onClick={() => setActiveTab('search')}
                >
                    🔍 Search
                </button>
                <button
                    className={`tab ${activeTab === 'health' ? 'active' : ''}`}
                    onClick={() => setActiveTab('health')}
                >
                    ❤️ Health
                </button>
                <button
                    className={`tab ${activeTab === 'stats' ? 'active' : ''}`}
                    onClick={() => setActiveTab('stats')}
                >
                    📊 Statistics
                </button>
                <a
                    href="patient_records.html"
                    className="tab"
                    style={{ textDecoration: 'none', color: 'inherit' }}
                >
                    📋 Patient Records
                </a>
            </nav>

            <main className="main">
                {activeTab === 'full' && <FullValidation />}
                {activeTab === 'fast' && <FastValidation />}
                {activeTab === 'search' && <Search />}
                {activeTab === 'health' && <SystemHealth />}
                {activeTab === 'stats' && <SystemStats />}
            </main>

            <footer className="footer">
                <p>KPME Provider Validation System v1.0.0 • Karnataka, India</p>
            </footer>
        </div>
    );
}

// Render the app
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
