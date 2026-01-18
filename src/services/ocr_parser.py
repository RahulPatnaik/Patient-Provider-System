"""
OCR Text Parser using Mistral AI
Converts raw OCR text into structured medical document data
"""

import os
import logging
from typing import Dict, Any, Optional
from mistralai import Mistral

logger = logging.getLogger(__name__)


class OCRParser:
    """
    AI-powered OCR text parser using Mistral AI

    Takes raw OCR text and extracts structured medical document fields
    """

    def __init__(self):
        self.api_key = os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            logger.warning("MISTRAL_API_KEY not configured. OCR parsing will use fallback regex.")
            self.client = None
        else:
            self.client = Mistral(api_key=self.api_key)
            logger.info("Mistral AI OCR parser initialized")

    async def parse_medical_document(self, raw_text: str) -> Dict[str, Any]:
        """
        Parse raw OCR text into structured medical document fields

        Args:
            raw_text: Raw text extracted from OCR

        Returns:
            Dictionary with structured fields
        """
        if not self.client:
            logger.warning("Mistral AI not available, using regex fallback")
            return self._parse_with_regex(raw_text)

        try:
            # Use Mistral AI to parse the text
            prompt = f"""You are a medical document parser. Extract structured information from the following medical document text.

The text is from an OCR scan of a medical prescription or certificate from India (Karnataka state).

Raw OCR Text:
```
{raw_text}
```

Please extract the following fields. If a field is not found, return empty string "":

1. patient_name - Patient's full name
2. doctor_name - Doctor's full name (look for "Dr." prefix)
3. clinic_name - Hospital/clinic/establishment name
4. clinic_location - City or address
5. phone - Phone number (10 digits, may start with 0 or +91)
6. email - Email address
7. prescription_date - Date of prescription (format: YYYY-MM-DD if possible)
8. medications - List of medications prescribed
9. diagnosis - Medical diagnosis or condition
10. certificate_number - Certificate/registration number (alphanumeric code)
11. license_number - Doctor's license/registration number
12. address - Full address if available
13. district - District name

Return ONLY a valid JSON object with these exact field names. Do not include any other text or explanation.

Example format:
{{
  "patient_name": "John Doe",
  "doctor_name": "Dr. Smith Kumar",
  "clinic_name": "City Hospital",
  "clinic_location": "Bangalore",
  "phone": "9876543210",
  "email": "contact@hospital.com",
  "prescription_date": "2026-01-18",
  "medications": "Paracetamol 500mg, Cetirizine 10mg",
  "diagnosis": "Common Cold",
  "certificate_number": "",
  "license_number": "KMC12345",
  "address": "123 MG Road, Bangalore",
  "district": "Bangalore Urban"
}}"""

            response = self.client.chat.complete(
                model="mistral-small-latest",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent extraction
                max_tokens=1000
            )

            # Parse JSON response
            import json
            result_text = response.choices[0].message.content.strip()

            # Remove markdown code blocks if present
            if result_text.startswith("```json"):
                result_text = result_text.replace("```json", "").replace("```", "").strip()
            elif result_text.startswith("```"):
                result_text = result_text.replace("```", "").strip()

            parsed_data = json.loads(result_text)

            logger.info(f"Mistral AI parsed {len([v for v in parsed_data.values() if v])} fields from OCR text")

            # Add raw text for reference
            parsed_data["raw_text"] = raw_text

            return parsed_data

        except Exception as e:
            logger.error(f"Mistral AI parsing failed: {e}, falling back to regex")
            return self._parse_with_regex(raw_text)

    def _parse_with_regex(self, text: str) -> Dict[str, Any]:
        """
        Fallback regex-based parser (legacy)
        Used when Mistral AI is not available
        """
        import re

        data = {
            "patient_name": "",
            "doctor_name": "",
            "clinic_name": "",
            "clinic_location": "",
            "phone": "",
            "email": "",
            "prescription_date": "",
            "medications": "",
            "diagnosis": "",
            "certificate_number": "",
            "license_number": "",
            "address": "",
            "district": "",
            "raw_text": text
        }

        # Normalize text
        text = text.replace('\n', ' ').replace('  ', ' ')
        text_lower = text.lower()

        # Extract phone number
        phone_match = re.search(r'\b(\+?91[-\s]?)?([6-9]\d{9})\b', text)
        if phone_match:
            data["phone"] = phone_match.group(2)

        # Extract email
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        if email_match:
            data["email"] = email_match.group(0)

        # Extract doctor name (look for "Dr." prefix)
        doctor_match = re.search(r'Dr\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', text)
        if doctor_match:
            data["doctor_name"] = "Dr. " + doctor_match.group(1)

        # Extract certificate number
        cert_match = re.search(r'\b([A-Z]{2,4}\d{4,10}[A-Z]{0,4})\b', text)
        if cert_match:
            data["certificate_number"] = cert_match.group(1)

        # Extract date
        date_patterns = [
            r'\b(\d{4}[-/]\d{2}[-/]\d{2})\b',  # YYYY-MM-DD or YYYY/MM/DD
            r'\b(\d{2}[-/]\d{2}[-/]\d{4})\b',  # DD-MM-YYYY or DD/MM/YYYY
        ]
        for pattern in date_patterns:
            date_match = re.search(pattern, text)
            if date_match:
                data["prescription_date"] = date_match.group(1)
                break

        # Extract address (lines with common address keywords)
        address_keywords = ['road', 'street', 'bangalore', 'karnataka', 'india', 'pin', 'layout']
        address_parts = []
        for line in text.split('.'):
            line_lower = line.strip().lower()
            if any(keyword in line_lower for keyword in address_keywords):
                address_parts.append(line.strip())
        if address_parts:
            data["address"] = ', '.join(address_parts[:3])  # Max 3 parts

        # Extract district
        districts = ['bangalore', 'mysore', 'mangalore', 'hubli', 'belgaum', 'gulbarga']
        for district in districts:
            if district in text_lower:
                data["district"] = district.capitalize()
                break

        logger.info(f"Regex parser extracted {len([v for v in data.values() if v])} fields")

        return data


# Singleton instance
_ocr_parser = None


def get_ocr_parser() -> OCRParser:
    """Get or create OCR parser singleton"""
    global _ocr_parser
    if _ocr_parser is None:
        _ocr_parser = OCRParser()
    return _ocr_parser
