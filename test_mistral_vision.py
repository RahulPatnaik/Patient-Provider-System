#!/usr/bin/env python3
"""
Test Mistral Vision OCR with P1.png prescription
"""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from services.mistral_vision_ocr import get_mistral_vision_ocr
import json


async def test_mistral_vision():
    print("\n" + "=" * 80)
    print("Testing Mistral Vision OCR with P1.png")
    print("=" * 80 + "\n")

    # Load prescription image
    prescription_path = Path("Prescriptions/P1.png")

    if not prescription_path.exists():
        print(f"❌ Prescription image not found: {prescription_path}")
        return

    print(f"📄 Loading image: {prescription_path}")

    with open(prescription_path, 'rb') as f:
        image_data = f.read()

    print(f"✅ Image loaded: {len(image_data)} bytes\n")

    # Extract with Mistral Vision
    print("🔍 Extracting text with Mistral Vision OCR (Pixtral model)...")

    try:
        mistral_ocr = get_mistral_vision_ocr()
        raw_text, structured_data = await mistral_ocr.extract_text_and_parse(image_data)

        print("\n" + "=" * 80)
        print("RAW TEXT EXTRACTED:")
        print("=" * 80)
        print(raw_text)

        print("\n" + "=" * 80)
        print("STRUCTURED DATA:")
        print("=" * 80)

        # Remove raw_text from structured display
        display_data = {k: v for k, v in structured_data.items() if k != 'raw_text' and v}

        print(json.dumps(display_data, indent=2))

        print("\n" + "=" * 80)
        print("SUMMARY:")
        print("=" * 80)

        print(f"\n✅ Raw text length: {len(raw_text)} characters")
        print(f"✅ Fields extracted: {len(display_data)}")

        print("\n📋 Key Information:")
        if structured_data.get('doctor_name'):
            print(f"   Doctor: {structured_data['doctor_name']}")
        if structured_data.get('doctor_license'):
            print(f"   License: {structured_data['doctor_license']}")
        if structured_data.get('doctor_phone'):
            print(f"   Phone: {structured_data['doctor_phone']}")

        if structured_data.get('clinic_name'):
            print(f"\n   Clinic: {structured_data['clinic_name']}")
        if structured_data.get('clinic_address'):
            print(f"   Address: {structured_data['clinic_address']}")

        if structured_data.get('patient_name'):
            print(f"\n   Patient: {structured_data['patient_name']}")
        if structured_data.get('patient_id'):
            print(f"   Patient ID: {structured_data['patient_id']}")
        if structured_data.get('patient_age'):
            print(f"   Age: {structured_data['patient_age']}")

        if structured_data.get('prescription_date'):
            print(f"\n   Date: {structured_data['prescription_date']}")

        if structured_data.get('medications'):
            print(f"\n   Medications:")
            meds = structured_data['medications']
            if isinstance(meds, list):
                for i, med in enumerate(meds, 1):
                    if isinstance(med, dict):
                        print(f"      {i}. {med.get('name', '')} {med.get('dosage', '')}")
                        if med.get('frequency'):
                            print(f"         {med.get('frequency')}")
                        if med.get('instructions'):
                            print(f"         ({med.get('instructions')})")
                    else:
                        print(f"      {i}. {med}")
            else:
                print(f"      {meds}")

        if structured_data.get('medications_text'):
            print(f"\n   Medications (formatted): {structured_data['medications_text']}")

        if structured_data.get('refills'):
            print(f"\n   Refills: {structured_data['refills']}")

        print("\n" + "=" * 80)
        print("✅ Test complete!")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_mistral_vision())
