"""
Patient Records API Routes
Comprehensive patient and prescription management with OCR integration
"""

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.patient_db import get_patient_db
from services.elasticsearch_service import get_elasticsearch_service
from services.ocr_service import get_ocr_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/patients", tags=["Patient Records"])


# ==================== PYDANTIC MODELS ====================

class PatientCreate(BaseModel):
    """Patient creation model"""
    patient_name: str = Field(..., min_length=2, max_length=200)
    date_of_birth: Optional[str] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_phone: Optional[str] = None
    blood_group: Optional[str] = None
    allergies: Optional[str] = None
    chronic_conditions: Optional[str] = None


class PatientUpdate(BaseModel):
    """Patient update model"""
    patient_name: Optional[str] = Field(None, min_length=2, max_length=200)
    date_of_birth: Optional[str] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_phone: Optional[str] = None
    blood_group: Optional[str] = None
    allergies: Optional[str] = None
    chronic_conditions: Optional[str] = None


class PatientResponse(BaseModel):
    """Patient response model"""
    id: int
    patient_name: str
    date_of_birth: Optional[str]
    gender: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    address: Optional[str]
    emergency_contact_name: Optional[str]
    emergency_contact_phone: Optional[str]
    blood_group: Optional[str]
    allergies: Optional[str]
    chronic_conditions: Optional[str]
    created_at: str
    updated_at: str


class PrescriptionCreate(BaseModel):
    """Prescription creation model"""
    patient_id: int = Field(..., gt=0)
    prescription_date: str
    doctor_name: str = Field(..., min_length=2)
    doctor_license: Optional[str] = None
    clinic_name: str = Field(..., min_length=2)
    clinic_location: Optional[str] = None
    clinic_phone: Optional[str] = None
    diagnosis: Optional[str] = None
    medications: str = Field(..., min_length=1)
    dosage_instructions: Optional[str] = None
    notes: Optional[str] = None


class PrescriptionResponse(BaseModel):
    """Prescription response model"""
    id: int
    patient_id: int
    prescription_date: str
    doctor_name: str
    doctor_license: Optional[str]
    clinic_name: str
    clinic_location: Optional[str]
    clinic_phone: Optional[str]
    diagnosis: Optional[str]
    medications: str
    dosage_instructions: Optional[str]
    notes: Optional[str]
    prescription_image_path: Optional[str]
    extracted_text: Optional[str]
    created_at: str


class PrescriptionWithPatient(PrescriptionResponse):
    """Prescription with patient name"""
    patient_name: Optional[str] = None


class OCRPrescriptionCreate(BaseModel):
    """Create prescription from OCR data"""
    patient_id: int = Field(..., gt=0)
    ocr_data: Dict[str, Any]
    extracted_text: str
    prescription_date: Optional[str] = None
    medications: Optional[str] = None


# ==================== PATIENT ENDPOINTS ====================

@router.post("/", response_model=Dict[str, Any], status_code=201)
async def create_patient(patient: PatientCreate):
    """
    Create a new patient record

    Stores patient demographics and medical information.
    Automatically indexes in Elasticsearch if available.
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Create patient
        patient_id = db.create_patient(patient.dict())

        # Index in Elasticsearch
        patient_data = db.get_patient(patient_id)
        if patient_data:
            es.index_patient(patient_data)

        logger.info(f"Created patient: {patient_id}")

        return {
            "success": True,
            "patient_id": patient_id,
            "message": "Patient record created successfully"
        }
    except Exception as e:
        logger.error(f"Failed to create patient: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{patient_id}", response_model=PatientResponse)
async def get_patient(patient_id: int):
    """Get patient by ID"""
    db = get_patient_db()
    patient = db.get_patient(patient_id)

    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    return patient


@router.put("/{patient_id}", response_model=Dict[str, Any])
async def update_patient(patient_id: int, patient_update: PatientUpdate):
    """
    Update patient record

    Updates demographics and re-indexes in Elasticsearch
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Check if patient exists
        existing = db.get_patient(patient_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Patient not found")

        # Update only provided fields
        update_data = {k: v for k, v in patient_update.dict().items() if v is not None}

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields to update")

        # Merge with existing data
        final_data = {**existing, **update_data}
        success = db.update_patient(patient_id, final_data)

        if success:
            # Re-index in Elasticsearch
            updated_patient = db.get_patient(patient_id)
            es.index_patient(updated_patient)

            return {
                "success": True,
                "message": "Patient updated successfully"
            }
        else:
            raise HTTPException(status_code=500, detail="Update failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update patient: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{patient_id}")
async def delete_patient(patient_id: int):
    """
    Delete patient record

    Deletes patient and all associated prescriptions.
    Also removes from Elasticsearch index.
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Check if patient exists
        patient = db.get_patient(patient_id)
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        # Delete patient (cascades to prescriptions)
        success = db.delete_patient(patient_id)

        if success:
            # Remove from Elasticsearch
            es.delete_patient(patient_id)

            return {
                "success": True,
                "message": "Patient and all prescriptions deleted successfully"
            }
        else:
            raise HTTPException(status_code=500, detail="Delete failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete patient: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[PatientResponse])
async def search_patients(
    query: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    limit: int = 50
):
    """
    Search patients

    Supports:
    - Full-text name search (with Elasticsearch if available)
    - Exact phone match
    - Email search
    - Combined filters
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Try Elasticsearch first if available and query provided
        if query and es.es_available:
            patient_ids = es.search_patients(query, limit)
            if patient_ids:
                # Fetch full patient data from database
                patients = [db.get_patient(pid) for pid in patient_ids]
                patients = [p for p in patients if p is not None]
                return patients

        # Fallback to database search
        patients = db.search_patients(query, phone, email, limit)
        return patients

    except Exception as e:
        logger.error(f"Patient search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== PRESCRIPTION ENDPOINTS ====================

@router.post("/{patient_id}/prescriptions", response_model=Dict[str, Any], status_code=201)
async def add_prescription(patient_id: int, prescription: PrescriptionCreate):
    """
    Add prescription for a patient

    Stores prescription details and indexes in Elasticsearch
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Verify patient exists
        patient = db.get_patient(patient_id)
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        # Override patient_id with path parameter
        prescription_data = prescription.dict()
        prescription_data['patient_id'] = patient_id

        # Create prescription
        prescription_id = db.add_prescription(prescription_data)

        # Index in Elasticsearch with patient name
        prescription_data['id'] = prescription_id
        prescription_data['patient_name'] = patient['patient_name']
        es.index_prescription(prescription_data)

        logger.info(f"Added prescription: {prescription_id} for patient: {patient_id}")

        return {
            "success": True,
            "prescription_id": prescription_id,
            "message": "Prescription added successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add prescription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{patient_id}/prescriptions/ocr", response_model=Dict[str, Any], status_code=201)
async def add_prescription_from_ocr(
    patient_id: int,
    file: UploadFile = File(..., description="Prescription image")
):
    """
    Create prescription from uploaded image using OCR

    Automatically extracts:
    - Doctor name and license
    - Clinic name and location
    - Prescription date
    - Medications and dosage
    - Diagnosis
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()
        ocr = get_ocr_service()

        # Verify patient exists
        patient = db.get_patient(patient_id)
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        # Validate file
        if not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")

        # Read image
        image_data = await file.read()

        # Use Mistral Vision OCR (best quality with built-in vision + parsing)
        try:
            from services.mistral_vision_ocr import get_mistral_vision_ocr
            mistral_ocr = get_mistral_vision_ocr()
            extracted_text, ocr_data = await mistral_ocr.extract_text_and_parse(image_data)
            provider = "mistral_vision"
        except Exception as mistral_error:
            logger.warning(f"Mistral Vision OCR failed, falling back to OCR.space: {mistral_error}")

            # Fallback to traditional OCR + parsing
            image_format = file.content_type.split('/')[-1]
            extracted_text, provider = await ocr.extract_text(image_data, image_format)

            from services.ocr_parser import get_ocr_parser
            ocr_parser = get_ocr_parser()
            ocr_data = await ocr_parser.parse_medical_document(extracted_text)

        # Build prescription data using AI-parsed fields
        # Handle both Mistral Vision format and legacy format
        prescription_data = {
            'patient_id': patient_id,
            'prescription_date': ocr_data.get('prescription_date') or date.today().isoformat(),
            'doctor_name': ocr_data.get('doctor_name') or 'Unknown Doctor',
            'doctor_license': ocr_data.get('doctor_license') or ocr_data.get('license_number'),
            'clinic_name': ocr_data.get('clinic_name') or 'Unknown Clinic',
            'clinic_location': (
                ocr_data.get('clinic_address') or
                ocr_data.get('clinic_location') or
                ocr_data.get('address') or
                ocr_data.get('district')
            ),
            'clinic_phone': ocr_data.get('clinic_phone') or ocr_data.get('phone') or ocr_data.get('doctor_phone'),
            'diagnosis': ocr_data.get('diagnosis') or '',
            'medications': (
                ocr_data.get('medications_text') or
                ocr_data.get('medications') or
                'See extracted text'
            ),
            'dosage_instructions': '',
            'notes': f'Extracted via {provider.upper()} OCR with AI vision parsing',
            'extracted_text': extracted_text
        }

        # Create prescription
        prescription_id = db.add_prescription(prescription_data)

        # Index in Elasticsearch
        prescription_data['id'] = prescription_id
        prescription_data['patient_name'] = patient['patient_name']
        es.index_prescription(prescription_data)

        logger.info(f"Added prescription from OCR: {prescription_id} for patient: {patient_id}")

        return {
            "success": True,
            "prescription_id": prescription_id,
            "ocr_provider": provider,
            "extracted_data": ocr_data,
            "extracted_text": extracted_text,
            "message": "Prescription created from OCR successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add prescription from OCR: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{patient_id}/prescriptions", response_model=List[PrescriptionResponse])
async def get_patient_prescriptions(patient_id: int, limit: int = 100):
    """
    Get all prescriptions for a patient

    Returns prescriptions ordered by date (newest first)
    """
    try:
        db = get_patient_db()

        # Verify patient exists
        patient = db.get_patient(patient_id)
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        prescriptions = db.get_patient_prescriptions(patient_id, limit)
        return prescriptions

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get prescriptions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prescriptions/{prescription_id}", response_model=PrescriptionResponse)
async def get_prescription(prescription_id: int):
    """Get prescription by ID"""
    db = get_patient_db()
    prescription = db.get_prescription(prescription_id)

    if not prescription:
        raise HTTPException(status_code=404, detail="Prescription not found")

    return prescription


@router.put("/prescriptions/{prescription_id}", response_model=Dict[str, Any])
async def update_prescription(prescription_id: int, prescription_update: PrescriptionCreate):
    """Update prescription record"""
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Check if prescription exists
        existing = db.get_prescription(prescription_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Prescription not found")

        # Update prescription
        success = db.update_prescription(prescription_id, prescription_update.dict())

        if success:
            # Re-index in Elasticsearch
            updated_prescription = db.get_prescription(prescription_id)
            patient = db.get_patient(updated_prescription['patient_id'])
            updated_prescription['patient_name'] = patient['patient_name'] if patient else ''
            es.index_prescription(updated_prescription)

            return {
                "success": True,
                "message": "Prescription updated successfully"
            }
        else:
            raise HTTPException(status_code=500, detail="Update failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update prescription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/prescriptions/{prescription_id}")
async def delete_prescription(prescription_id: int):
    """Delete prescription record"""
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Check if prescription exists
        prescription = db.get_prescription(prescription_id)
        if not prescription:
            raise HTTPException(status_code=404, detail="Prescription not found")

        # Delete prescription
        success = db.delete_prescription(prescription_id)

        if success:
            # Remove from Elasticsearch
            es.delete_prescription(prescription_id)

            return {
                "success": True,
                "message": "Prescription deleted successfully"
            }
        else:
            raise HTTPException(status_code=500, detail="Delete failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete prescription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prescriptions/search/", response_model=List[PrescriptionWithPatient])
async def search_prescriptions(
    query: Optional[str] = None,
    doctor_name: Optional[str] = None,
    clinic_name: Optional[str] = None,
    clinic_location: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100
):
    """
    Search prescriptions

    Supports:
    - Full-text search (query parameter) - searches doctor, clinic, diagnosis, medications
    - Specific filters: doctor_name, clinic_name, clinic_location
    - Date range: date_from, date_to (YYYY-MM-DD format)
    - Elasticsearch-powered when available for better search quality
    """
    try:
        db = get_patient_db()
        es = get_elasticsearch_service()

        # Try Elasticsearch first if available
        if es.es_available and (query or doctor_name or clinic_name or clinic_location):
            prescription_ids = es.search_prescriptions(
                query, doctor_name, clinic_name, clinic_location, limit
            )
            if prescription_ids:
                # Fetch full prescription data from database
                prescriptions = [db.get_prescription(pid) for pid in prescription_ids]
                prescriptions = [p for p in prescriptions if p is not None]
                return prescriptions

        # Fallback to database search
        prescriptions = db.search_prescriptions(
            doctor_name, clinic_name, clinic_location, date_from, date_to, limit
        )
        return prescriptions

    except Exception as e:
        logger.error(f"Prescription search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/summary")
async def get_patient_stats():
    """Get patient records statistics"""
    try:
        db = get_patient_db()
        stats = db.get_stats()

        return {
            "success": True,
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
