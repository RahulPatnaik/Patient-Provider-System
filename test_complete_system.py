#!/usr/bin/env python3
"""
Complete System Test
Tests OCR, patient records, and provider search functionality
"""

import asyncio
import requests
import json
from pathlib import Path

API_BASE_URL = "http://localhost:8000"


def print_header(text):
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def print_success(text):
    print(f"✅ {text}")


def print_error(text):
    print(f"❌ {text}")


def print_info(text):
    print(f"ℹ️  {text}")


def test_provider_search():
    """Test provider search with various queries"""
    print_header("Testing Provider Search")

    test_queries = [
        ("HOSPITAL", "Should find multiple hospital results"),
        ("CLINIC", "Should find multiple clinic results"),
        ("AAROGYA", "Should find AAROGYA establishments"),
    ]

    for query, description in test_queries:
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/admin/search/establishments",
                params={"query": query, "limit": 100}
            )
            data = response.json()

            if response.status_code == 200:
                count = data.get('total_results', 0)
                print_success(f"Query '{query}': {count} results - {description}")
                if count > 0:
                    print_info(f"   First result: {data['results'][0].get('establishment_name', 'N/A')}")
            else:
                print_error(f"Query '{query}' failed: {response.status_code}")
        except Exception as e:
            print_error(f"Query '{query}' error: {str(e)}")


def test_ocr_extraction():
    """Test OCR extraction with prescription image"""
    print_header("Testing OCR Extraction")

    # Check if prescription image exists
    prescription_path = Path("/home/rahul/Desktop/Patient-Provider-System/Prescriptions/P1.png")

    if not prescription_path.exists():
        print_error(f"Prescription image not found: {prescription_path}")
        return None

    print_info(f"Testing with image: {prescription_path}")

    try:
        with open(prescription_path, 'rb') as f:
            files = {'file': ('prescription.png', f, 'image/png')}
            response = requests.post(
                f"{API_BASE_URL}/api/v1/ocr/extract",
                files=files
            )

        if response.status_code == 200:
            data = response.json()
            print_success("OCR extraction successful")
            print_info(f"   Provider: {data.get('provider')}")
            print_info(f"   Confidence: {data.get('confidence')}")
            print_info(f"   Text length: {len(data.get('extracted_text', ''))} characters")

            structured_data = data.get('structured_data', {})
            fields_found = {k: v for k, v in structured_data.items() if v and k != 'raw_text'}
            print_info(f"   Fields extracted: {len(fields_found)}")

            for key, value in fields_found.items():
                if key != 'raw_text':
                    print_info(f"      {key}: {value[:50]}..." if len(str(value)) > 50 else f"      {key}: {value}")

            return data
        else:
            print_error(f"OCR extraction failed: {response.status_code}")
            print_error(f"   Response: {response.text}")
            return None
    except Exception as e:
        print_error(f"OCR extraction error: {str(e)}")
        return None


def test_patient_crud():
    """Test patient CRUD operations"""
    print_header("Testing Patient CRUD Operations")

    # Create patient
    patient_data = {
        "patient_name": "Test Patient OCR",
        "date_of_birth": "1985-05-20",
        "gender": "Female",
        "phone": "9123456789",
        "email": "test.ocr@example.com",
        "blood_group": "AB+",
        "allergies": "Penicillin",
        "chronic_conditions": "Hypertension"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/v1/patients/",
            json=patient_data
        )

        if response.status_code == 201:
            data = response.json()
            patient_id = data.get('patient_id')
            print_success(f"Patient created: ID {patient_id}")

            # Get patient
            response = requests.get(f"{API_BASE_URL}/api/v1/patients/{patient_id}")
            if response.status_code == 200:
                print_success(f"Patient retrieved successfully")
            else:
                print_error(f"Failed to retrieve patient")

            # Search patient
            response = requests.get(
                f"{API_BASE_URL}/api/v1/patients/",
                params={"query": "Test Patient"}
            )
            if response.status_code == 200:
                data = response.json()
                print_success(f"Patient search: found {len(data)} results")

            return patient_id
        else:
            print_error(f"Failed to create patient: {response.status_code}")
            print_error(f"   Response: {response.text}")
            return None
    except Exception as e:
        print_error(f"Patient CRUD error: {str(e)}")
        return None


def test_prescription_ocr(patient_id):
    """Test prescription creation from OCR"""
    print_header("Testing Prescription OCR")

    if not patient_id:
        print_error("No patient ID provided, skipping prescription test")
        return

    prescription_path = Path("/home/rahul/Desktop/Patient-Provider-System/Prescriptions/P1.png")

    if not prescription_path.exists():
        print_error(f"Prescription image not found: {prescription_path}")
        return

    try:
        with open(prescription_path, 'rb') as f:
            files = {'file': ('prescription.png', f, 'image/png')}
            response = requests.post(
                f"{API_BASE_URL}/api/v1/patients/{patient_id}/prescriptions/ocr",
                files=files
            )

        if response.status_code == 201:
            data = response.json()
            print_success("Prescription created from OCR")
            print_info(f"   Prescription ID: {data.get('prescription_id')}")
            print_info(f"   OCR Provider: {data.get('ocr_provider')}")

            extracted_data = data.get('extracted_data', {})
            print_info(f"   Doctor: {extracted_data.get('doctor_name', 'N/A')}")
            print_info(f"   Clinic: {extracted_data.get('clinic_name', 'N/A')}")
            print_info(f"   Medications: {extracted_data.get('medications', 'N/A')[:50]}...")

            # Get prescriptions
            response = requests.get(
                f"{API_BASE_URL}/api/v1/patients/{patient_id}/prescriptions"
            )
            if response.status_code == 200:
                prescriptions = response.json()
                print_success(f"Patient has {len(prescriptions)} prescription(s)")
        else:
            print_error(f"Failed to create prescription from OCR: {response.status_code}")
            print_error(f"   Response: {response.text}")
    except Exception as e:
        print_error(f"Prescription OCR error: {str(e)}")


def test_statistics():
    """Test statistics endpoints"""
    print_header("Testing Statistics")

    try:
        # Patient stats
        response = requests.get(f"{API_BASE_URL}/api/v1/patients/stats/summary")
        if response.status_code == 200:
            data = response.json()
            stats = data.get('stats', {})
            print_success("Patient statistics retrieved")
            print_info(f"   Total Patients: {stats.get('total_patients')}")
            print_info(f"   Total Prescriptions: {stats.get('total_prescriptions')}")
            print_info(f"   Total Doctors: {stats.get('total_doctors')}")
            print_info(f"   Total Clinics: {stats.get('total_clinics')}")

        # KPME stats
        response = requests.get(f"{API_BASE_URL}/api/v1/admin/stats")
        if response.status_code == 200:
            data = response.json()
            print_success("KPME statistics retrieved")
            print_info(f"   Establishments: {data.get('total_establishments')}")
            print_info(f"   Staff: {data.get('total_staff')}")
    except Exception as e:
        print_error(f"Statistics error: {str(e)}")


def main():
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 20 + "COMPLETE SYSTEM TEST SUITE" + " " * 32 + "║")
    print("╚" + "═" * 78 + "╝")

    # Check if server is running
    try:
        response = requests.get(f"{API_BASE_URL}/api/v1/health")
        if response.status_code == 200:
            print_success("Backend server is running")
        else:
            print_error("Backend server not responding correctly")
            return
    except Exception as e:
        print_error(f"Cannot connect to backend server at {API_BASE_URL}")
        print_error(f"   Please start the server first: PYTHONPATH=src python src/main.py")
        return

    # Run tests
    test_provider_search()
    ocr_result = test_ocr_extraction()
    patient_id = test_patient_crud()

    if patient_id:
        test_prescription_ocr(patient_id)

    test_statistics()

    print("\n" + "=" * 80)
    print("  Test Suite Complete")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
