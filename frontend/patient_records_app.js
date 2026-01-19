/**
 * Patient Records Management System - React Application
 * Comprehensive patient and prescription management with OCR integration
 */

const { useState, useEffect } = React;

const API_BASE_URL = 'http://localhost:8000';

// ==================== MAIN APP COMPONENT ====================

function PatientRecordsApp() {
    const [activeTab, setActiveTab] = useState('patients');
    const [patients, setPatients] = useState([]);
    const [stats, setStats] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        loadStats();
        if (activeTab === 'patients') {
            loadPatients();
        }
    }, [activeTab]);

    const loadStats = async () => {
        try {
            const response = await fetch(`${API_BASE_URL}/api/v1/patients/stats/summary`);
            const data = await response.json();
            setStats(data.stats);
        } catch (err) {
            console.error('Failed to load stats:', err);
        }
    };

    const loadPatients = async (searchQuery = '') => {
        setLoading(true);
        setError(null);
        try {
            let url = `${API_BASE_URL}/api/v1/patients/`;
            if (searchQuery) {
                url += `?query=${encodeURIComponent(searchQuery)}`;
            }
            const response = await fetch(url);
            const data = await response.json();
            setPatients(data);
        } catch (err) {
            setError('Failed to load patients');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="patient-records-page">
            <a href="index.html" className="back-link">← Back to KPME Validation</a>

            <div className="page-header">
                <h1>Patient Records System</h1>
                <p>Comprehensive medical records management with OCR integration</p>
            </div>

            {stats && <StatsOverview stats={stats} />}

            <div className="tabs">
                <div
                    className={`tab ${activeTab === 'patients' ? 'active' : ''}`}
                    onClick={() => setActiveTab('patients')}
                >
                    All Patients
                </div>
                <div
                    className={`tab ${activeTab === 'add-patient' ? 'active' : ''}`}
                    onClick={() => setActiveTab('add-patient')}
                >
                    Add New Patient
                </div>
                <div
                    className={`tab ${activeTab === 'search' ? 'active' : ''}`}
                    onClick={() => setActiveTab('search')}
                >
                    Search Records
                </div>
            </div>

            {error && (
                <div style={{
                    padding: '15px',
                    background: '#fee',
                    border: '2px solid #c33',
                    color: '#c33',
                    marginBottom: '20px',
                    fontFamily: 'var(--font-mono)'
                }}>
                    {error}
                </div>
            )}

            {activeTab === 'patients' && (
                <PatientList
                    patients={patients}
                    loading={loading}
                    onRefresh={loadPatients}
                    onSearch={loadPatients}
                />
            )}

            {activeTab === 'add-patient' && (
                <PatientForm
                    onSuccess={() => {
                        setActiveTab('patients');
                        loadPatients();
                        loadStats();
                    }}
                />
            )}

            {activeTab === 'search' && (
                <SearchRecords />
            )}
        </div>
    );
}

// ==================== STATS OVERVIEW COMPONENT ====================

function StatsOverview({ stats }) {
    return (
        <div className="stats-grid">
            <div className="stat-card">
                <div className="stat-value">{stats.total_patients || 0}</div>
                <div className="stat-label">Total Patients</div>
            </div>
            <div className="stat-card">
                <div className="stat-value">{stats.total_prescriptions || 0}</div>
                <div className="stat-label">Prescriptions</div>
            </div>
            <div className="stat-card">
                <div className="stat-value">{stats.total_doctors || 0}</div>
                <div className="stat-label">Doctors</div>
            </div>
            <div className="stat-card">
                <div className="stat-value">{stats.total_clinics || 0}</div>
                <div className="stat-label">Clinics</div>
            </div>
            <div className="stat-card">
                <div className="stat-value">{stats.prescriptions_last_30_days || 0}</div>
                <div className="stat-label">Last 30 Days</div>
            </div>
        </div>
    );
}

// ==================== PATIENT FORM COMPONENT ====================

function PatientForm({ patientData = null, onSuccess }) {
    const [formData, setFormData] = useState({
        patient_name: '',
        date_of_birth: '',
        gender: '',
        phone: '',
        email: '',
        address: '',
        emergency_contact_name: '',
        emergency_contact_phone: '',
        blood_group: '',
        allergies: '',
        chronic_conditions: '',
        medical_notes: ''
    });
    const [ocrFile, setOcrFile] = useState(null);
    const [ocrLoading, setOcrLoading] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [message, setMessage] = useState(null);

    useEffect(() => {
        if (patientData) {
            setFormData(patientData);
        }
    }, [patientData]);

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleOcrUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        setOcrFile(file);
        setOcrLoading(true);
        setMessage(null);

        try {
            const formDataOcr = new FormData();
            formDataOcr.append('file', file);

            const response = await fetch(`${API_BASE_URL}/api/v1/ocr/extract`, {
                method: 'POST',
                body: formDataOcr
            });

            const data = await response.json();

            if (data.success) {
                // Auto-fill form with OCR data - clean up Mistral Vision prefixes
                const sd = data.structured_data;

                // Extract clean patient name (remove "PATIENT: " prefix)
                let patientName = sd.patient_name || '';
                if (patientName.startsWith('PATIENT:')) {
                    patientName = patientName.replace('PATIENT:', '').trim();
                }

                // Extract clean address (remove "ADDRESS: " prefix)
                let address = sd.patient_address || sd.address || '';
                if (address.startsWith('ADDRESS:')) {
                    address = address.replace('ADDRESS:', '').trim();
                }

                // Convert DOB format if needed (MM/DD/YYYY -> YYYY-MM-DD)
                let dob = sd.patient_dob || '';
                if (dob.startsWith('DOB:')) {
                    dob = dob.replace('DOB:', '').trim();
                }
                // Parse MM/DD/YYYY to YYYY-MM-DD
                if (dob && dob.match(/\d{2}\/\d{2}\/\d{4}/)) {
                    const [month, day, year] = dob.split('/');
                    dob = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
                }

                // Extract phone (from doctor, clinic, or patient)
                let phone = sd.clinic_phone || sd.doctor_phone || sd.phone || '';
                if (phone.startsWith('TEL:')) {
                    phone = phone.replace('TEL:', '').replace(/[^\d]/g, '').trim();
                } else {
                    phone = phone.replace(/[^\d]/g, ''); // Remove all non-digits
                }
                // Take last 10 digits if longer
                if (phone.length > 10) {
                    phone = phone.slice(-10);
                }

                // Build medical notes from prescription data
                let medicalNotes = '';
                if (sd.doctor_name) {
                    medicalNotes += `Doctor: ${sd.doctor_name}\n`;
                }
                if (sd.doctor_license) {
                    medicalNotes += `License: ${sd.doctor_license}\n`;
                }
                if (sd.clinic_name) {
                    medicalNotes += `Clinic: ${sd.clinic_name}\n`;
                }
                if (sd.clinic_address) {
                    medicalNotes += `Clinic Address: ${sd.clinic_address}\n`;
                }
                if (sd.prescription_date) {
                    medicalNotes += `\nPrescription Date: ${sd.prescription_date}\n`;
                }
                if (sd.diagnosis) {
                    medicalNotes += `Diagnosis: ${sd.diagnosis}\n`;
                }
                if (sd.medications_text || sd.medications) {
                    medicalNotes += `\nMedications:\n${sd.medications_text || JSON.stringify(sd.medications, null, 2)}\n`;
                }
                if (sd.refills) {
                    medicalNotes += `${sd.refills}\n`;
                }
                if (sd.notes) {
                    medicalNotes += `\nNotes: ${sd.notes}\n`;
                }

                setFormData(prev => ({
                    ...prev,
                    patient_name: patientName || prev.patient_name,
                    date_of_birth: dob || prev.date_of_birth,
                    phone: phone || prev.phone,
                    address: address || prev.address,
                    medical_notes: medicalNotes || prev.medical_notes
                }));

                setMessage({
                    type: 'success',
                    text: `OCR extraction successful using ${data.provider}. Auto-filled: ${patientName ? 'name' : ''}${dob ? ', DOB' : ''}${phone ? ', phone' : ''}${address ? ', address' : ''}${medicalNotes ? ', medical notes (prescription details)' : ''}.`
                });
            } else {
                setMessage({
                    type: 'error',
                    text: 'OCR extraction failed. Please enter details manually.'
                });
            }
        } catch (err) {
            console.error('OCR error:', err);
            setMessage({
                type: 'error',
                text: 'OCR upload failed. Please enter details manually.'
            });
        } finally {
            setOcrLoading(false);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setSubmitting(true);
        setMessage(null);

        try {
            const url = patientData
                ? `${API_BASE_URL}/api/v1/patients/${patientData.id}`
                : `${API_BASE_URL}/api/v1/patients/`;

            const method = patientData ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(formData)
            });

            const data = await response.json();

            if (data.success) {
                setMessage({
                    type: 'success',
                    text: patientData ? 'Patient updated successfully!' : 'Patient created successfully!'
                });

                if (onSuccess) {
                    setTimeout(() => onSuccess(), 1500);
                }
            } else {
                setMessage({
                    type: 'error',
                    text: data.detail || 'Operation failed'
                });
            }
        } catch (err) {
            console.error('Submit error:', err);
            setMessage({
                type: 'error',
                text: 'Failed to save patient record'
            });
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="patient-form">
            <h2 style={{ fontFamily: 'var(--font-code)', color: 'var(--green-primary)', marginBottom: '20px' }}>
                {patientData ? 'Update Patient' : 'Add New Patient'}
            </h2>

            {/* OCR Upload Section */}
            {!patientData && (
                <div style={{
                    marginBottom: '30px',
                    padding: '20px',
                    background: 'white',
                    border: 'var(--border-medium) solid var(--saffron-primary)'
                }}>
                    <h3 style={{ fontFamily: 'var(--font-mono)', color: 'var(--saffron-primary)', marginBottom: '10px' }}>
                        Option 1: Extract from Prescription (OCR)
                    </h3>
                    <input
                        type="file"
                        accept="image/*"
                        onChange={handleOcrUpload}
                        disabled={ocrLoading}
                        style={{
                            padding: '10px',
                            border: 'var(--border-medium) solid var(--border-color)',
                            fontFamily: 'var(--font-mono)',
                            width: '100%'
                        }}
                    />
                    {ocrLoading && (
                        <p style={{ marginTop: '10px', color: 'var(--saffron-primary)', fontFamily: 'var(--font-mono)' }}>
                            Extracting data from prescription...
                        </p>
                    )}
                </div>
            )}

            {message && (
                <div style={{
                    padding: '15px',
                    marginBottom: '20px',
                    background: message.type === 'success' ? '#dfd' : '#fee',
                    border: `2px solid ${message.type === 'success' ? '#3a3' : '#c33'}`,
                    color: message.type === 'success' ? '#3a3' : '#c33',
                    fontFamily: 'var(--font-mono)'
                }}>
                    {message.text}
                </div>
            )}

            <form onSubmit={handleSubmit}>
                <h3 style={{ fontFamily: 'var(--font-mono)', color: 'var(--green-primary)', marginBottom: '15px' }}>
                    {patientData ? 'Patient Information' : 'Option 2: Enter Manually'}
                </h3>

                <div className="form-grid">
                    <div className="form-field">
                        <label>Patient Name *</label>
                        <input
                            type="text"
                            name="patient_name"
                            value={formData.patient_name}
                            onChange={handleInputChange}
                            required
                        />
                    </div>

                    <div className="form-field">
                        <label>Date of Birth</label>
                        <input
                            type="date"
                            name="date_of_birth"
                            value={formData.date_of_birth}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Gender</label>
                        <select
                            name="gender"
                            value={formData.gender}
                            onChange={handleInputChange}
                        >
                            <option value="">Select Gender</option>
                            <option value="Male">Male</option>
                            <option value="Female">Female</option>
                            <option value="Other">Other</option>
                        </select>
                    </div>

                    <div className="form-field">
                        <label>Phone</label>
                        <input
                            type="tel"
                            name="phone"
                            value={formData.phone}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Email</label>
                        <input
                            type="email"
                            name="email"
                            value={formData.email}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Blood Group</label>
                        <select
                            name="blood_group"
                            value={formData.blood_group}
                            onChange={handleInputChange}
                        >
                            <option value="">Select Blood Group</option>
                            <option value="A+">A+</option>
                            <option value="A-">A-</option>
                            <option value="B+">B+</option>
                            <option value="B-">B-</option>
                            <option value="AB+">AB+</option>
                            <option value="AB-">AB-</option>
                            <option value="O+">O+</option>
                            <option value="O-">O-</option>
                        </select>
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Address</label>
                        <textarea
                            name="address"
                            value={formData.address}
                            onChange={handleInputChange}
                            rows="2"
                        />
                    </div>

                    <div className="form-field">
                        <label>Emergency Contact Name</label>
                        <input
                            type="text"
                            name="emergency_contact_name"
                            value={formData.emergency_contact_name}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Emergency Contact Phone</label>
                        <input
                            type="tel"
                            name="emergency_contact_phone"
                            value={formData.emergency_contact_phone}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Allergies</label>
                        <textarea
                            name="allergies"
                            value={formData.allergies}
                            onChange={handleInputChange}
                            rows="2"
                            placeholder="List any known allergies..."
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Chronic Conditions</label>
                        <textarea
                            name="chronic_conditions"
                            value={formData.chronic_conditions}
                            onChange={handleInputChange}
                            rows="2"
                            placeholder="List any chronic medical conditions..."
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Medical Notes / Doctor's Notes</label>
                        <textarea
                            name="medical_notes"
                            value={formData.medical_notes}
                            onChange={handleInputChange}
                            rows="6"
                            placeholder="Prescription details, doctor's notes, medications, etc. (auto-filled from OCR)"
                            style={{ fontFamily: 'monospace', fontSize: '0.9rem' }}
                        />
                    </div>
                </div>

                <div className="action-buttons">
                    <button
                        type="submit"
                        className="btn btn-primary"
                        disabled={submitting}
                    >
                        {submitting ? 'Saving...' : (patientData ? 'Update Patient' : 'Create Patient')}
                    </button>
                </div>
            </form>
        </div>
    );
}

// ==================== PATIENT LIST COMPONENT ====================

function PatientList({ patients, loading, onRefresh, onSearch }) {
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedPatient, setSelectedPatient] = useState(null);

    const handleSearch = (e) => {
        e.preventDefault();
        onSearch(searchQuery);
    };

    if (selectedPatient) {
        return (
            <PatientDetail
                patientId={selectedPatient}
                onBack={() => {
                    setSelectedPatient(null);
                    onRefresh();
                }}
            />
        );
    }

    return (
        <div className="search-section">
            <form className="search-bar" onSubmit={handleSearch}>
                <input
                    type="text"
                    placeholder="Search patients by name, phone, or email..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                />
                <button type="submit" className="btn btn-primary">
                    Search
                </button>
                <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => {
                        setSearchQuery('');
                        onRefresh('');
                    }}
                >
                    Clear
                </button>
            </form>

            {loading ? (
                <div className="empty-state">Loading patients...</div>
            ) : patients.length === 0 ? (
                <div className="empty-state">
                    No patients found. Add your first patient to get started.
                </div>
            ) : (
                <div className="patient-list">
                    {patients.map(patient => (
                        <div key={patient.id} className="patient-card">
                            <div className="patient-card-header">
                                <h3>{patient.patient_name}</h3>
                                <span className="badge badge-green">ID: {patient.id}</span>
                            </div>
                            <div className="patient-details">
                                {patient.date_of_birth && (
                                    <div className="detail-item">
                                        <span className="detail-label">DOB:</span>
                                        <span>{patient.date_of_birth}</span>
                                    </div>
                                )}
                                {patient.gender && (
                                    <div className="detail-item">
                                        <span className="detail-label">Gender:</span>
                                        <span>{patient.gender}</span>
                                    </div>
                                )}
                                {patient.phone && (
                                    <div className="detail-item">
                                        <span className="detail-label">Phone:</span>
                                        <span>{patient.phone}</span>
                                    </div>
                                )}
                                {patient.email && (
                                    <div className="detail-item">
                                        <span className="detail-label">Email:</span>
                                        <span>{patient.email}</span>
                                    </div>
                                )}
                                {patient.blood_group && (
                                    <div className="detail-item">
                                        <span className="detail-label">Blood Group:</span>
                                        <span>{patient.blood_group}</span>
                                    </div>
                                )}
                            </div>
                            <div className="action-buttons" style={{ marginTop: '15px' }}>
                                <button
                                    className="btn btn-primary"
                                    onClick={() => setSelectedPatient(patient.id)}
                                >
                                    View Details
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// ==================== PATIENT DETAIL COMPONENT ====================

function PatientDetail({ patientId, onBack }) {
    const [patient, setPatient] = useState(null);
    const [prescriptions, setPrescriptions] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showAddPrescription, setShowAddPrescription] = useState(false);
    const [editMode, setEditMode] = useState(false);

    useEffect(() => {
        loadPatientData();
    }, [patientId]);

    const loadPatientData = async () => {
        setLoading(true);
        try {
            // Load patient details
            const patientResponse = await fetch(`${API_BASE_URL}/api/v1/patients/${patientId}`);
            const patientData = await patientResponse.json();
            setPatient(patientData);

            // Load prescriptions
            const prescriptionsResponse = await fetch(
                `${API_BASE_URL}/api/v1/patients/${patientId}/prescriptions`
            );
            const prescriptionsData = await prescriptionsResponse.json();
            setPrescriptions(prescriptionsData);
        } catch (err) {
            console.error('Failed to load patient data:', err);
        } finally {
            setLoading(false);
        }
    };

    const handleDeletePatient = async () => {
        if (!confirm('Are you sure you want to delete this patient and all prescriptions?')) {
            return;
        }

        try {
            const response = await fetch(`${API_BASE_URL}/api/v1/patients/${patientId}`, {
                method: 'DELETE'
            });

            const data = await response.json();
            if (data.success) {
                alert('Patient deleted successfully');
                onBack();
            }
        } catch (err) {
            console.error('Delete failed:', err);
            alert('Failed to delete patient');
        }
    };

    if (loading) {
        return <div className="empty-state">Loading patient details...</div>;
    }

    if (!patient) {
        return <div className="empty-state">Patient not found</div>;
    }

    if (editMode) {
        return (
            <PatientForm
                patientData={patient}
                onSuccess={() => {
                    setEditMode(false);
                    loadPatientData();
                }}
            />
        );
    }

    if (showAddPrescription) {
        return (
            <PrescriptionForm
                patientId={patientId}
                patientName={patient.patient_name}
                onSuccess={() => {
                    setShowAddPrescription(false);
                    loadPatientData();
                }}
                onCancel={() => setShowAddPrescription(false)}
            />
        );
    }

    return (
        <div>
            <div className="action-buttons" style={{ marginBottom: '20px' }}>
                <button className="btn btn-secondary" onClick={onBack}>
                    ← Back to List
                </button>
                <button className="btn btn-primary" onClick={() => setEditMode(true)}>
                    Edit Patient
                </button>
                <button className="btn btn-danger" onClick={handleDeletePatient}>
                    Delete Patient
                </button>
            </div>

            <div className="patient-form">
                <h2 style={{ fontFamily: 'var(--font-code)', color: 'var(--green-primary)' }}>
                    {patient.patient_name}
                    <span className="badge" style={{ marginLeft: '15px' }}>ID: {patient.id}</span>
                </h2>

                <div className="form-grid" style={{ marginTop: '20px' }}>
                    <div className="detail-item">
                        <span className="detail-label">Date of Birth:</span>
                        <span>{patient.date_of_birth || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Gender:</span>
                        <span>{patient.gender || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Phone:</span>
                        <span>{patient.phone || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Email:</span>
                        <span>{patient.email || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Blood Group:</span>
                        <span>{patient.blood_group || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Address:</span>
                        <span>{patient.address || 'Not provided'}</span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Emergency Contact:</span>
                        <span>
                            {patient.emergency_contact_name || 'Not provided'}
                            {patient.emergency_contact_phone && ` (${patient.emergency_contact_phone})`}
                        </span>
                    </div>
                    <div className="detail-item">
                        <span className="detail-label">Allergies:</span>
                        <span>{patient.allergies || 'None reported'}</span>
                    </div>
                    <div className="detail-item" style={{ gridColumn: '1 / -1' }}>
                        <span className="detail-label">Chronic Conditions:</span>
                        <span>{patient.chronic_conditions || 'None reported'}</span>
                    </div>
                    {patient.medical_notes && (
                        <div className="detail-item" style={{ gridColumn: '1 / -1' }}>
                            <span className="detail-label">Medical Notes:</span>
                            <pre style={{
                                whiteSpace: 'pre-wrap',
                                fontFamily: 'monospace',
                                fontSize: '0.85rem',
                                margin: '10px 0',
                                padding: '10px',
                                background: '#f5f5f5',
                                borderLeft: '3px solid var(--green-primary)'
                            }}>
                                {patient.medical_notes}
                            </pre>
                        </div>
                    )}
                </div>
            </div>

            <div className="prescription-list">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
                    <h2 style={{ fontFamily: 'var(--font-code)', color: 'var(--saffron-primary)', margin: 0 }}>
                        Prescriptions ({prescriptions.length})
                    </h2>
                    <button
                        className="btn btn-primary"
                        onClick={() => setShowAddPrescription(true)}
                    >
                        + Add Prescription
                    </button>
                </div>

                {prescriptions.length === 0 ? (
                    <div className="empty-state">
                        No prescriptions recorded yet. Add the first prescription.
                    </div>
                ) : (
                    prescriptions.map(prescription => (
                        <div key={prescription.id} className="prescription-item">
                            <div className="prescription-header">
                                <span className="prescription-date">
                                    {prescription.prescription_date}
                                </span>
                                <span className="prescription-doctor">
                                    Dr. {prescription.doctor_name}
                                </span>
                            </div>
                            <div className="prescription-details">
                                <p><strong>Clinic:</strong> {prescription.clinic_name}</p>
                                {prescription.clinic_location && (
                                    <p><strong>Location:</strong> {prescription.clinic_location}</p>
                                )}
                                {prescription.diagnosis && (
                                    <p><strong>Diagnosis:</strong> {prescription.diagnosis}</p>
                                )}
                                <p><strong>Medications:</strong> {prescription.medications}</p>
                                {prescription.dosage_instructions && (
                                    <p><strong>Dosage:</strong> {prescription.dosage_instructions}</p>
                                )}
                                {prescription.notes && (
                                    <p><strong>Notes:</strong> {prescription.notes}</p>
                                )}
                            </div>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
}

// ==================== PRESCRIPTION FORM COMPONENT ====================

function PrescriptionForm({ patientId, patientName, onSuccess, onCancel }) {
    const [formData, setFormData] = useState({
        prescription_date: new Date().toISOString().split('T')[0],
        doctor_name: '',
        doctor_license: '',
        clinic_name: '',
        clinic_location: '',
        clinic_phone: '',
        diagnosis: '',
        medications: '',
        dosage_instructions: '',
        notes: ''
    });
    const [ocrFile, setOcrFile] = useState(null);
    const [ocrLoading, setOcrLoading] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [message, setMessage] = useState(null);

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleOcrUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        setOcrFile(file);
        setOcrLoading(true);
        setMessage(null);

        try {
            const formDataOcr = new FormData();
            formDataOcr.append('file', file);

            const response = await fetch(
                `${API_BASE_URL}/api/v1/patients/${patientId}/prescriptions/ocr`,
                {
                    method: 'POST',
                    body: formDataOcr
                }
            );

            const data = await response.json();

            if (data.success) {
                setMessage({
                    type: 'success',
                    text: `Prescription created from OCR using ${data.ocr_provider}!`
                });

                setTimeout(() => {
                    if (onSuccess) onSuccess();
                }, 1500);
            } else {
                setMessage({
                    type: 'error',
                    text: 'OCR upload failed. Please enter details manually.'
                });
            }
        } catch (err) {
            console.error('OCR error:', err);
            setMessage({
                type: 'error',
                text: 'OCR upload failed. Please enter details manually.'
            });
        } finally {
            setOcrLoading(false);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setSubmitting(true);
        setMessage(null);

        try {
            const response = await fetch(
                `${API_BASE_URL}/api/v1/patients/${patientId}/prescriptions`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ ...formData, patient_id: patientId })
                }
            );

            const data = await response.json();

            if (data.success) {
                setMessage({
                    type: 'success',
                    text: 'Prescription added successfully!'
                });

                setTimeout(() => {
                    if (onSuccess) onSuccess();
                }, 1500);
            } else {
                setMessage({
                    type: 'error',
                    text: data.detail || 'Failed to add prescription'
                });
            }
        } catch (err) {
            console.error('Submit error:', err);
            setMessage({
                type: 'error',
                text: 'Failed to add prescription'
            });
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="prescription-form">
            <h2 style={{ fontFamily: 'var(--font-code)', color: 'var(--green-primary)', marginBottom: '10px' }}>
                Add Prescription for {patientName}
            </h2>

            {message && (
                <div style={{
                    padding: '15px',
                    marginBottom: '20px',
                    background: message.type === 'success' ? '#dfd' : '#fee',
                    border: `2px solid ${message.type === 'success' ? '#3a3' : '#c33'}`,
                    color: message.type === 'success' ? '#3a3' : '#c33',
                    fontFamily: 'var(--font-mono)'
                }}>
                    {message.text}
                </div>
            )}

            {/* OCR Upload Section */}
            <div style={{
                marginBottom: '30px',
                padding: '20px',
                background: 'white',
                border: 'var(--border-medium) solid var(--saffron-primary)'
            }}>
                <h3 style={{ fontFamily: 'var(--font-mono)', color: 'var(--saffron-primary)', marginBottom: '10px' }}>
                    Option 1: Upload Prescription Image (OCR)
                </h3>
                <input
                    type="file"
                    accept="image/*"
                    onChange={handleOcrUpload}
                    disabled={ocrLoading}
                    style={{
                        padding: '10px',
                        border: 'var(--border-medium) solid var(--border-color)',
                        fontFamily: 'var(--font-mono)',
                        width: '100%'
                    }}
                />
                {ocrLoading && (
                    <p style={{ marginTop: '10px', color: 'var(--saffron-primary)', fontFamily: 'var(--font-mono)' }}>
                        Extracting prescription data...
                    </p>
                )}
            </div>

            <form onSubmit={handleSubmit}>
                <h3 style={{ fontFamily: 'var(--font-mono)', color: 'var(--green-primary)', marginBottom: '15px' }}>
                    Option 2: Enter Manually
                </h3>

                <div className="form-grid">
                    <div className="form-field">
                        <label>Prescription Date *</label>
                        <input
                            type="date"
                            name="prescription_date"
                            value={formData.prescription_date}
                            onChange={handleInputChange}
                            required
                        />
                    </div>

                    <div className="form-field">
                        <label>Doctor Name *</label>
                        <input
                            type="text"
                            name="doctor_name"
                            value={formData.doctor_name}
                            onChange={handleInputChange}
                            required
                        />
                    </div>

                    <div className="form-field">
                        <label>Doctor License Number</label>
                        <input
                            type="text"
                            name="doctor_license"
                            value={formData.doctor_license}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Clinic Name *</label>
                        <input
                            type="text"
                            name="clinic_name"
                            value={formData.clinic_name}
                            onChange={handleInputChange}
                            required
                        />
                    </div>

                    <div className="form-field">
                        <label>Clinic Location</label>
                        <input
                            type="text"
                            name="clinic_location"
                            value={formData.clinic_location}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field">
                        <label>Clinic Phone</label>
                        <input
                            type="tel"
                            name="clinic_phone"
                            value={formData.clinic_phone}
                            onChange={handleInputChange}
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Diagnosis</label>
                        <textarea
                            name="diagnosis"
                            value={formData.diagnosis}
                            onChange={handleInputChange}
                            rows="2"
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Medications *</label>
                        <textarea
                            name="medications"
                            value={formData.medications}
                            onChange={handleInputChange}
                            rows="3"
                            required
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Dosage Instructions</label>
                        <textarea
                            name="dosage_instructions"
                            value={formData.dosage_instructions}
                            onChange={handleInputChange}
                            rows="2"
                        />
                    </div>

                    <div className="form-field" style={{ gridColumn: '1 / -1' }}>
                        <label>Notes</label>
                        <textarea
                            name="notes"
                            value={formData.notes}
                            onChange={handleInputChange}
                            rows="2"
                        />
                    </div>
                </div>

                <div className="action-buttons">
                    <button
                        type="submit"
                        className="btn btn-primary"
                        disabled={submitting}
                    >
                        {submitting ? 'Adding...' : 'Add Prescription'}
                    </button>
                    <button
                        type="button"
                        className="btn btn-secondary"
                        onClick={onCancel}
                    >
                        Cancel
                    </button>
                </div>
            </form>
        </div>
    );
}

// ==================== SEARCH RECORDS COMPONENT ====================

function SearchRecords() {
    const [searchType, setSearchType] = useState('patient');
    const [searchQuery, setSearchQuery] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);

    const handleSearch = async (e) => {
        e.preventDefault();
        if (!searchQuery.trim()) return;

        setLoading(true);
        try {
            if (searchType === 'patient') {
                const response = await fetch(
                    `${API_BASE_URL}/api/v1/patients/?query=${encodeURIComponent(searchQuery)}`
                );
                const data = await response.json();
                setResults(data);
            } else {
                const response = await fetch(
                    `${API_BASE_URL}/api/v1/patients/prescriptions/search/?query=${encodeURIComponent(searchQuery)}`
                );
                const data = await response.json();
                setResults(data);
            }
        } catch (err) {
            console.error('Search failed:', err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="search-section">
            <h2 style={{ fontFamily: 'var(--font-code)', color: 'var(--green-primary)', marginBottom: '20px' }}>
                Advanced Search
            </h2>

            <form className="search-bar" onSubmit={handleSearch}>
                <select
                    value={searchType}
                    onChange={(e) => setSearchType(e.target.value)}
                    style={{
                        padding: '12px',
                        border: 'var(--border-medium) solid var(--green-primary)',
                        fontFamily: 'var(--font-mono)',
                        fontSize: '1rem'
                    }}
                >
                    <option value="patient">Search Patients</option>
                    <option value="prescription">Search Prescriptions</option>
                </select>
                <input
                    type="text"
                    placeholder={
                        searchType === 'patient'
                            ? "Search by name, phone, email..."
                            : "Search by doctor, clinic, medications..."
                    }
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                />
                <button type="submit" className="btn btn-primary">
                    Search
                </button>
            </form>

            {loading ? (
                <div className="empty-state">Searching...</div>
            ) : results.length === 0 && searchQuery ? (
                <div className="empty-state">No results found</div>
            ) : results.length > 0 ? (
                <div style={{ marginTop: '30px' }}>
                    <h3 style={{ fontFamily: 'var(--font-mono)', color: 'var(--saffron-primary)' }}>
                        Found {results.length} result(s)
                    </h3>
                    <div className="patient-list" style={{ marginTop: '20px' }}>
                        {searchType === 'patient' ? (
                            results.map(patient => (
                                <div key={patient.id} className="patient-card">
                                    <div className="patient-card-header">
                                        <h3>{patient.patient_name}</h3>
                                        <span className="badge">ID: {patient.id}</span>
                                    </div>
                                    <div className="patient-details">
                                        {patient.phone && (
                                            <div className="detail-item">
                                                <span className="detail-label">Phone:</span>
                                                <span>{patient.phone}</span>
                                            </div>
                                        )}
                                        {patient.email && (
                                            <div className="detail-item">
                                                <span className="detail-label">Email:</span>
                                                <span>{patient.email}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))
                        ) : (
                            results.map(prescription => (
                                <div key={prescription.id} className="prescription-item">
                                    <div className="prescription-header">
                                        <span className="prescription-date">
                                            {prescription.prescription_date}
                                        </span>
                                        <span className="prescription-doctor">
                                            Patient: {prescription.patient_name}
                                        </span>
                                    </div>
                                    <div className="prescription-details">
                                        <p><strong>Doctor:</strong> {prescription.doctor_name}</p>
                                        <p><strong>Clinic:</strong> {prescription.clinic_name}</p>
                                        {prescription.clinic_location && (
                                            <p><strong>Location:</strong> {prescription.clinic_location}</p>
                                        )}
                                        <p><strong>Medications:</strong> {prescription.medications}</p>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>
            ) : null}
        </div>
    );
}

// ==================== RENDER APP ====================

ReactDOM.render(<PatientRecordsApp />, document.getElementById('root'));
