"""
Chatbot API routes - Help users understand the system
Uses Mistral AI for conversational assistance
"""

import os
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from mistralai import Mistral

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/chatbot", tags=["chatbot"])


class ChatMessage(BaseModel):
    """Chat message model"""
    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatRequest(BaseModel):
    """Chat request model"""
    messages: List[ChatMessage] = Field(..., description="Conversation history")
    language: str = Field(default="en", description="User's preferred language (en, hi, kn, ta, te, etc.)")

    class Config:
        json_schema_extra = {
            "example": {
                "messages": [
                    {"role": "user", "content": "How do I validate a provider?"}
                ],
                "language": "en"
            }
        }


class ChatResponse(BaseModel):
    """Chat response model"""
    message: str = Field(..., description="Assistant's response")
    success: bool = Field(default=True, description="Whether the request succeeded")


# System context about the Patient-Provider system
SYSTEM_CONTEXT_TEMPLATE = """You are a helpful assistant for the KPME Patient-Provider Validation System. Your role is to help users understand how to use the system.

IMPORTANT: The user's preferred language is {language}. Please respond in {language_name} to match their preference.

## System Overview:
This is a healthcare provider validation and patient records management system for Karnataka, India.

## Main Features:

### 1. Provider Validation
- **Fast Validation**: Quick KPME certificate verification (deterministic, no AI)
  - Go to main page
  - Enter certificate number and provider name
  - Get instant validation result

- **Full Validation**: Comprehensive multi-agent validation
  - Includes KPME check, compliance, data quality assessment
  - Takes 2-6 seconds
  - Provides detailed confidence scoring

### 2. Patient Records
- **Add Patients**: Create patient records with basic information
  - Name, date of birth, phone, address, medical notes

- **OCR Prescription Upload**: Upload prescription images
  - Auto-fills patient details from prescription
  - Uses Mistral Vision AI for text extraction
  - Extracts: patient info, doctor info, medications, diagnosis

- **Search & Manage**: Search patients and view their records

### 3. KPME Database
- 956 healthcare establishments in Karnataka
- 30 districts covered
- Searchable by name, certificate, phone
- Data refreshed from official KPME portal

## Common Questions:

**Q: How do I validate a provider?**
A: Go to the main page, enter the certificate number (e.g., BLU01453ALCDS) and provider name, then click "Validate Provider".

**Q: What's the difference between Fast and Full validation?**
A: Fast validation is instant (0.4ms) and uses only database lookup. Full validation takes 2-6 seconds and includes AI-powered compliance checks, data enrichment, and multi-source verification.

**Q: How do I add a patient?**
A: Go to Patient Records page, click "Add Patient", fill in the form. You can also upload a prescription image to auto-fill details.

**Q: How does OCR work?**
A: Upload a prescription image (PNG, JPG, PDF). The system uses Mistral Vision to extract text and automatically fills patient name, DOB, phone, address, and medical notes (medications, doctor info, etc.).

**Q: What if a provider fails validation?**
A: Check the validation details. Common issues: certificate expired, missing required fields (phone, district), or compliance issues. You may need to add more information and retry.

**Q: How do I search for providers?**
A: Use the admin search endpoint or the main validation form. You can search by establishment name, certificate number, or phone.

**Q: What API endpoints are available?**
A: See the API documentation at http://localhost:8000/docs for full list of endpoints including validation, patient management, OCR, and admin functions.

Be friendly, concise, and helpful. If you don't know something specific, suggest checking the API docs or main pages."""


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Chat with the assistant about the system

    Uses Mistral AI to provide helpful responses about using the system.
    Supports multiple languages - Mistral will respond in the user's preferred language.
    """
    try:
        # Get Mistral API key
        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=500,
                detail="Mistral API key not configured"
            )

        # Initialize Mistral client
        client = Mistral(api_key=api_key)

        # Language mapping for better context
        language_names = {
            "en": "English",
            "hi": "Hindi (हिंदी)",
            "kn": "Kannada (ಕನ್ನಡ)",
            "ta": "Tamil (தமிழ்)",
            "te": "Telugu (తెలుగు)",
            "mr": "Marathi (मराठी)",
            "bn": "Bengali (বাংলা)",
            "gu": "Gujarati (ગુજરાતી)"
        }
        
        language_name = language_names.get(request.language, "English")
        
        # Prepare messages with language-aware system context
        system_context = SYSTEM_CONTEXT_TEMPLATE.format(
            language=request.language,
            language_name=language_name
        )
        
        messages = [
            {"role": "system", "content": system_context}
        ]

        # Add conversation history
        for msg in request.messages:
            messages.append({
                "role": msg.role,
                "content": msg.content
            })

        # Call Mistral AI
        response = client.chat.complete(
            model="mistral-small-latest",
            messages=messages,
            temperature=0.7,
            max_tokens=500
        )

        # Extract response
        assistant_message = response.choices[0].message.content

        logger.info(f"Chatbot response generated (length: {len(assistant_message)})")

        return ChatResponse(
            message=assistant_message,
            success=True
        )

    except Exception as e:
        logger.error(f"Chatbot error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Chatbot error: {str(e)}"
        )


@router.get("/health")
async def chatbot_health():
    """Check chatbot health"""
    api_key = os.getenv("MISTRAL_API_KEY")

    return {
        "status": "healthy" if api_key else "unavailable",
        "mistral_configured": bool(api_key),
        "model": "mistral-small-latest"
    }
