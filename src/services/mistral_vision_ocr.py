"""
Mistral Vision OCR Service
Uses Mistral AI's Pixtral vision model for superior OCR quality
"""

import os
import base64
import logging
from typing import Tuple, Dict, Any
from mistralai import Mistral

logger = logging.getLogger(__name__)


class MistralVisionOCR:
    """
    Mistral Vision OCR service using Pixtral model

    Provides high-quality OCR with built-in vision capabilities
    Better accuracy than OCR.space and Google Vision for medical documents
    """

    def __init__(self):
        self.api_key = os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            logger.error("MISTRAL_API_KEY not configured")
            self.client = None
        else:
            self.client = Mistral(api_key=self.api_key)
            logger.info("Mistral Vision OCR initialized with Pixtral model")

    async def extract_text_and_parse(self, image_data: bytes) -> Tuple[str, Dict[str, Any]]:
        """
        Extract text from image and parse into structured medical data

        Args:
            image_data: Image bytes

        Returns:
            Tuple of (raw_text, structured_data)
        """
        if not self.client:
            raise Exception("Mistral API key not configured")

        try:
            # Encode image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')

            # Comprehensive medical document extraction prompt
            prompt = """You are a medical document OCR specialist. Extract ALL information from this prescription/medical document image with extreme accuracy.

Please extract the following information:

**Doctor Information:**
- Doctor's full name (look for "Dr." or "DR.")
- Doctor's license/registration number
- Doctor's phone number
- Doctor's email

**Clinic/Hospital Information:**
- Clinic/Hospital name
- Full address (street, city, state, PIN/ZIP)
- Phone number
- Email

**Patient Information:**
- Patient's full name
- Patient ID/Medical Record Number
- Age
- Date of Birth
- Gender
- Address

**Prescription Details:**
- Prescription date (extract exact date)
- Diagnosis/Condition
- All medications with:
  * Medication name
  * Dosage/Strength
  * Frequency (how often to take)
  * Special instructions
  * Duration
- Number of refills
- Any additional notes or warnings

**Certificate/Registration:**
- Any certificate numbers
- Registration numbers
- License numbers

Return TWO things:
1. First, the COMPLETE RAW TEXT exactly as it appears on the document (every single word and number)
2. Then, a structured JSON object with all the extracted fields

Format your response EXACTLY like this:

RAW_TEXT:
[Put all the text you can read from the image here, line by line, exactly as it appears]

STRUCTURED_DATA:
{
  "doctor_name": "",
  "doctor_license": "",
  "doctor_phone": "",
  "doctor_email": "",
  "clinic_name": "",
  "clinic_address": "",
  "clinic_phone": "",
  "clinic_email": "",
  "patient_name": "",
  "patient_id": "",
  "patient_age": "",
  "patient_dob": "",
  "patient_gender": "",
  "patient_address": "",
  "prescription_date": "",
  "diagnosis": "",
  "medications": [
    {
      "name": "",
      "dosage": "",
      "frequency": "",
      "instructions": "",
      "duration": ""
    }
  ],
  "refills": "",
  "notes": "",
  "certificate_number": ""
}

Be extremely thorough and accurate. Extract EVERY piece of text visible on the document."""

            # Call Mistral Vision API with Pixtral model
            response = self.client.chat.complete(
                model="pixtral-12b-2409",  # Pixtral vision model
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            },
                            {
                                "type": "image_url",
                                "image_url": f"data:image/png;base64,{image_base64}"
                            }
                        ]
                    }
                ],
                temperature=0.0,  # Maximum consistency
                max_tokens=2000
            )

            result_text = response.choices[0].message.content.strip()

            # Parse the response
            raw_text = ""
            structured_data = {}

            if "RAW_TEXT:" in result_text and "STRUCTURED_DATA:" in result_text:
                parts = result_text.split("STRUCTURED_DATA:")
                raw_text = parts[0].replace("RAW_TEXT:", "").strip()

                # Extract JSON from structured data part
                json_part = parts[1].strip()

                # Remove markdown code blocks if present
                if json_part.startswith("```json"):
                    json_part = json_part.replace("```json", "").replace("```", "").strip()
                elif json_part.startswith("```"):
                    json_part = json_part.replace("```", "").strip()

                import json
                structured_data = json.loads(json_part)
            else:
                # Fallback: treat entire response as raw text
                raw_text = result_text
                structured_data = self._parse_fallback(result_text)

            logger.info(f"Mistral Vision OCR: Extracted {len(raw_text)} chars of raw text")
            logger.info(f"Mistral Vision OCR: Parsed {len([v for v in structured_data.values() if v])} fields")

            # Flatten medications list for easier consumption
            if 'medications' in structured_data and isinstance(structured_data['medications'], list):
                medications_text = []
                for med in structured_data['medications']:
                    if isinstance(med, dict):
                        med_str = f"{med.get('name', '')} {med.get('dosage', '')}"
                        if med.get('frequency'):
                            med_str += f" - {med.get('frequency')}"
                        if med.get('instructions'):
                            med_str += f" ({med.get('instructions')})"
                        medications_text.append(med_str.strip())

                structured_data['medications_text'] = '; '.join(medications_text)

            # Add raw text to structured data
            structured_data['raw_text'] = raw_text

            return raw_text, structured_data

        except Exception as e:
            logger.error(f"Mistral Vision OCR failed: {e}", exc_info=True)
            raise Exception(f"Mistral Vision OCR failed: {str(e)}")

    def _parse_fallback(self, text: str) -> Dict[str, Any]:
        """Fallback parser if structured extraction fails"""
        import re

        data = {
            "doctor_name": "",
            "doctor_license": "",
            "clinic_name": "",
            "patient_name": "",
            "prescription_date": "",
            "medications_text": "",
            "raw_text": text
        }

        # Extract doctor name
        doctor_match = re.search(r'Dr\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', text)
        if doctor_match:
            data["doctor_name"] = "Dr. " + doctor_match.group(1)

        # Extract patient name
        patient_match = re.search(r'Patient:?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', text, re.IGNORECASE)
        if patient_match:
            data["patient_name"] = patient_match.group(1)

        # Extract date
        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text)
        if date_match:
            data["prescription_date"] = date_match.group(1)

        return data


# Singleton instance
_mistral_vision_ocr = None


def get_mistral_vision_ocr() -> MistralVisionOCR:
    """Get or create Mistral Vision OCR singleton"""
    global _mistral_vision_ocr
    if _mistral_vision_ocr is None:
        _mistral_vision_ocr = MistralVisionOCR()
    return _mistral_vision_ocr
