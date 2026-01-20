"""
Speech-to-Text API route using Groq Whisper
"""

import os
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/stt", tags=["speech-to-text"])


class TranscriptionResponse(BaseModel):
    """Transcription response model"""
    text: str
    language: Optional[str] = None
    success: bool = True


@router.post("/transcribe", response_model=TranscriptionResponse)
async def transcribe_audio(file: UploadFile = File(...)):
    """
    Transcribe audio to text using Groq Whisper Large V3 Turbo
    
    Accepts audio files in various formats (mp3, wav, m4a, webm, etc.)
    Returns: Transcribed text in English
    """
    try:
        # Get Groq API key
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=500,
                detail="Groq API key not configured"
            )
        
        # Read audio file
        audio_data = await file.read()
        
        if not audio_data:
            raise HTTPException(
                status_code=400,
                detail="Empty audio file"
            )
        
        # Prepare request to Groq Whisper API
        url = "https://api.groq.com/openai/v1/audio/transcriptions"
        
        # Create multipart form data
        files = {
            'file': (file.filename or 'audio.webm', audio_data, file.content_type or 'audio/webm')
        }
        
        data = {
            'model': 'whisper-large-v3-turbo',
            'language': 'en',  # English only as per requirement
            'response_format': 'json'
        }
        
        headers = {
            'Authorization': f'Bearer {api_key}'
        }
        
        # Make request to Groq API
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                files=files,
                data=data,
                headers=headers
            )
        
        if response.status_code != 200:
            error_detail = response.text
            logger.error(f"Groq API error: {error_detail}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Transcription failed: {error_detail}"
            )
        
        result = response.json()
        transcribed_text = result.get('text', '')
        
        logger.info(f"Audio transcribed successfully: {len(transcribed_text)} characters")
        
        return TranscriptionResponse(
            text=transcribed_text,
            language='en',
            success=True
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Transcription error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Transcription error: {str(e)}"
        )


@router.get("/health")
async def stt_health():
    """Check STT service health"""
    api_key = os.getenv("GROQ_API_KEY")
    
    return {
        "status": "healthy" if api_key else "unavailable",
        "groq_configured": bool(api_key),
        "model": "whisper-large-v3-turbo"
    }
