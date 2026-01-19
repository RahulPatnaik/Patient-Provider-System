"""
OCR API Routes
Endpoints for optical character recognition of medical documents
"""

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Optional, Any
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.ocr_service import get_ocr_service
from services.ocr_parser import get_ocr_parser
from services.mistral_vision_ocr import get_mistral_vision_ocr

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ocr", tags=["OCR"])


class OCRResponse(BaseModel):
    """OCR extraction response"""
    success: bool
    extracted_text: str
    structured_data: Dict[str, Any]  # Changed to Any to support lists/dicts from Mistral Vision
    provider: str
    confidence: Optional[float] = None


class StructuredData(BaseModel):
    """Parsed structured data from medical document"""
    establishment_name: str = ""
    certificate_number: str = ""
    phone: str = ""
    email: str = ""
    address: str = ""
    district: str = ""
    doctor_name: str = ""
    license_number: str = ""
    patient_name: str = ""
    date: str = ""


@router.post("/extract", response_model=OCRResponse)
async def extract_text_from_image(
    file: UploadFile = File(..., description="Image file (PNG, JPG, PDF)")
):
    """
    Extract text from uploaded medical document using OCR

    Supports multiple OCR providers with automatic fallback:
    1. Google Cloud Vision API (best quality, 99%+ accuracy)
    2. OCR.space API (free tier, good quality)

    Returns both raw extracted text and parsed structured data.
    """
    try:
        # Validate file type
        if not file.content_type.startswith(('image/', 'application/pdf')):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type: {file.content_type}. Must be image or PDF."
            )

        # Read file data
        image_data = await file.read()

        if len(image_data) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

        if len(image_data) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="File too large. Maximum 10MB.")

        # Determine image format
        image_format = file.content_type.split('/')[-1]
        if image_format == 'jpeg':
            image_format = 'jpg'

        # Use Mistral Vision OCR (best quality with built-in vision + parsing)
        try:
            mistral_ocr = get_mistral_vision_ocr()
            extracted_text, structured_data = await mistral_ocr.extract_text_and_parse(image_data)
            provider = "mistral_vision"
        except Exception as mistral_error:
            logger.warning(f"Mistral Vision OCR failed, falling back to OCR.space: {mistral_error}")

            # Fallback to traditional OCR + parsing
            ocr_service = get_ocr_service()
            extracted_text, provider = await ocr_service.extract_text(image_data, image_format)

            # Parse structured data using AI-powered parser
            ocr_parser = get_ocr_parser()
            structured_data = await ocr_parser.parse_medical_document(extracted_text)

        logger.info(
            f"OCR extraction successful using {provider}, "
            f"extracted {len(extracted_text)} characters, "
            f"found {sum(1 for v in structured_data.values() if v)} fields"
        )

        return OCRResponse(
            success=True,
            extracted_text=extracted_text,
            structured_data=structured_data,
            provider=provider,
            confidence=0.95 if provider == "google_vision" else 0.85
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OCR extraction failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"OCR extraction failed: {str(e)}"
        )


def parse_medical_document(text: str) -> Dict[str, str]:
    """
    Parse extracted text into structured medical document fields

    Supports:
    - Karnataka medical establishment certificates
    - Doctor licenses
    - Prescriptions
    - Patient records
    """
    import re

    data = {
        "establishment_name": "",
        "certificate_number": "",
        "phone": "",
        "email": "",
        "address": "",
        "district": "",
        "doctor_name": "",
        "license_number": "",
        "patient_name": "",
        "date": "",
    }

    # Normalize text (handle OCR artifacts)
    text = text.replace('\n', ' ').replace('  ', ' ')
    original_text = text
    text_lower = text.lower()

    # Extract certificate number (Karnataka format: BDR00172ALHL2)
    cert_patterns = [
        r'\b([A-Z]{2,4}\d{4,10}[A-Z]{0,4})\b',  # Standard format
        r'certificate[:\s#]+([A-Z0-9]{8,15})',  # "Certificate: XXX"
        r'cert[.\s#]+([A-Z0-9]{8,15})',         # "Cert. XXX"
    ]
    for pattern in cert_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["certificate_number"] = match.group(1) if len(match.groups()) == 1 else match.group(0)
            break

    # Extract license number
    license_patterns = [
        r'license[:\s#]+([A-Z0-9\s]{5,20})',
        r'registration[:\s#]+([A-Z0-9\s]{5,20})',
        r'reg[.\s#]+([A-Z0-9\s]{5,20})',
    ]
    for pattern in license_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["license_number"] = match.group(1).strip()
            break

    # Extract Indian phone number (10 digits, starts with 6-9)
    phone_patterns = [
        r'(?:\+91|0)?[\s-]?([6-9]\d{9})\b',  # Standard format
        r'phone[:\s]+(?:\+91|0)?[\s-]?([6-9]\d{9})',  # "Phone: XXX"
        r'mobile[:\s]+(?:\+91|0)?[\s-]?([6-9]\d{9})',  # "Mobile: XXX"
        r'contact[:\s]+(?:\+91|0)?[\s-]?([6-9]\d{9})',  # "Contact: XXX"
    ]
    for pattern in phone_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["phone"] = match.group(1)
            break

    # Extract email
    email_match = re.search(r'\b([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})\b', text, re.IGNORECASE)
    if email_match:
        data["email"] = email_match.group(1).lower()

    # Extract doctor name
    doctor_patterns = [
        r'(?:dr|doctor)[.\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})',  # "Dr. Name"
        r'\bDR[.\s]+([A-Z\s]{3,30})',  # "DR. NAME" (caps)
    ]
    for pattern in doctor_patterns:
        match = re.search(pattern, text)
        if match:
            data["doctor_name"] = match.group(1).strip().title()
            break

    # Extract patient name
    patient_patterns = [
        r'patient[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})',  # "Patient: Name"
        r'name[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})',  # "Name: XXX"
    ]
    for pattern in patient_patterns:
        match = re.search(pattern, text)
        if match and match.group(1).strip() != data["doctor_name"]:
            data["patient_name"] = match.group(1).strip()
            break

    # Extract hospital/establishment name
    establishment_patterns = [
        r'(?:hospital|clinic|medical center|healthcare|nursing home)[:\s]*([^\n,]{3,50})',
        r'establishment[:\s]*([^\n,]{3,50})',
        r'\b([A-Z][A-Z\s]{5,40}(?:HOSPITAL|CLINIC|MEDICAL|HEALTHCARE))',
    ]
    for pattern in establishment_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).strip().title()
            if len(name) > 3 and name not in data.values():
                data["establishment_name"] = name
                break

    # Extract Karnataka district
    districts = [
        'Bangalore Urban', 'Bangalore Rural', 'Mysore', 'Mysuru', 'Mangalore', 'Hubli',
        'Dharwad', 'Belgaum', 'Belagavi', 'Gulbarga', 'Kalaburagi', 'Bellary', 'Ballari',
        'Tumkur', 'Tumakuru', 'Shimoga', 'Shivamogga', 'Davangere', 'Bijapur', 'Vijayapura',
        'Raichur', 'Udupi', 'Hassan', 'Chitradurga', 'Kolar', 'Mandya', 'Chikmagalur',
        'Chikkamagaluru', 'Bidar', 'Bagalkot', 'Gadag', 'Haveri', 'Koppal', 'Yadgir',
        'Chamarajanagar', 'Dakshina Kannada', 'Uttara Kannada', 'Kodagu', 'Chikkaballapur'
    ]

    text_for_district = original_text + ' ' + text_lower
    for district in districts:
        # Check both original case and lowercase
        if district.lower() in text_lower or district in original_text:
            data["district"] = district
            break

    # Extract date (multiple formats)
    date_patterns = [
        r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',  # DD/MM/YYYY or MM/DD/YYYY
        r'\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b',    # YYYY-MM-DD
        r'date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',  # "Date: XX/XX/XXXX"
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["date"] = match.group(1) if '/' in match.group(1) or '-' in match.group(1) else match.group(0)
            break

    # Extract address (complex pattern)
    address_patterns = [
        r'address[:\s]+([^\n]{10,100})',
        r'(?:located at|situated at)[:\s]+([^\n]{10,100})',
        r'(\d+[,\s]+[A-Za-z\s]+(?:street|road|avenue|st|rd|ave)[,\s]+[^\n]{5,80})',
    ]
    for pattern in address_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            addr = match.group(1).strip()
            # Clean up common OCR artifacts
            addr = re.sub(r'\s+', ' ', addr)
            if len(addr) > 10:
                data["address"] = addr
                break

    # Log parsed data for debugging
    found_fields = [k for k, v in data.items() if v]
    logger.info(f"Parsed {len(found_fields)} fields: {', '.join(found_fields)}")

    return data


@router.get("/health")
async def ocr_health():
    """Check OCR service health and available providers"""
    import os
    ocr_service = get_ocr_service()

    providers = {
        "mistral_vision": bool(os.getenv("MISTRAL_API_KEY")),
        "ocrspace": bool(ocr_service.ocrspace_api_key),
    }

    available_providers = [name for name, available in providers.items() if available]

    return {
        "status": "healthy",
        "providers": providers,
        "available_providers": available_providers,
        "primary_provider": available_providers[0] if available_providers else "none"
    }
