"""
OCR Service - Multiple Provider Support
Supports: Google Cloud Vision API, OCR.space, Tesseract (fallback)
"""

import os
import base64
import logging
from typing import Dict, Optional, Tuple
from pathlib import Path
from io import BytesIO
from PIL import Image

logger = logging.getLogger(__name__)


class OCRService:
    """Multi-provider OCR service with intelligent fallback"""

    def __init__(self):
        self.google_credentials_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/home/rahul/Desktop/Patient-Provider-System/thsemel-484714-40ab6456a1aa.json"
        )
        self.ocrspace_api_key = os.getenv("OCRSPACE_API_KEY", "K87899142388957")  # Free API key

    def _compress_image(self, image_data: bytes, max_size_kb: int = 900) -> bytes:
        """
        Compress image to meet size requirements while maintaining OCR quality

        Args:
            image_data: Original image bytes
            max_size_kb: Maximum size in KB

        Returns:
            Compressed image bytes
        """
        try:
            # Open image
            img = Image.open(BytesIO(image_data))

            # Convert to RGB if needed (remove alpha channel)
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                img = background

            # Start with high quality
            quality = 95
            output = BytesIO()

            # Iteratively reduce quality until size is acceptable
            while quality > 20:
                output.seek(0)
                output.truncate()

                # Save with current quality
                img.save(output, format='JPEG', quality=quality, optimize=True)
                size_kb = len(output.getvalue()) / 1024

                logger.debug(f"Compressed image to {size_kb:.1f}KB at quality {quality}")

                if size_kb <= max_size_kb:
                    logger.info(f"Image compressed successfully: {len(image_data)/1024:.1f}KB → {size_kb:.1f}KB")
                    return output.getvalue()

                # Reduce quality for next iteration
                quality -= 10

            # If still too large, resize the image
            logger.warning("Quality reduction insufficient, resizing image")
            max_dimension = 2000
            img.thumbnail((max_dimension, max_dimension), Image.Resampling.LANCZOS)

            output.seek(0)
            output.truncate()
            img.save(output, format='JPEG', quality=85, optimize=True)

            final_size_kb = len(output.getvalue()) / 1024
            logger.info(f"Image resized and compressed: {len(image_data)/1024:.1f}KB → {final_size_kb:.1f}KB")

            return output.getvalue()

        except Exception as e:
            logger.error(f"Image compression failed: {e}")
            return image_data  # Return original if compression fails

    async def extract_text(self, image_data: bytes, image_format: str = "png") -> Tuple[str, str]:
        """
        Extract text from image using best available OCR provider

        Args:
            image_data: Image bytes
            image_format: Image format (png, jpg, pdf)

        Returns:
            Tuple of (extracted_text, provider_used)
        """
        # Try providers in order of quality
        providers = [
            ("google_vision", self._google_cloud_vision),
            ("ocrspace", self._ocrspace_api),
        ]

        for provider_name, provider_func in providers:
            try:
                logger.info(f"Attempting OCR with provider: {provider_name}")
                text = await provider_func(image_data, image_format)
                if text and len(text.strip()) > 10:  # Minimum viable text
                    logger.info(f"OCR successful with {provider_name}, extracted {len(text)} characters")
                    return text, provider_name
            except Exception as e:
                logger.warning(f"OCR provider {provider_name} failed: {e}")
                continue

        # All providers failed
        raise Exception("All OCR providers failed. Please check image quality and try again.")

    async def _google_cloud_vision(self, image_data: bytes, image_format: str) -> str:
        """Google Cloud Vision API - Best quality (99%+ accuracy), uses service account JSON"""
        if not os.path.exists(self.google_credentials_path):
            raise Exception(f"Google Cloud Vision credentials not found at {self.google_credentials_path}")

        try:
            # Import Google Cloud Vision client library
            from google.cloud import vision
            import json

            # Load credentials
            with open(self.google_credentials_path, 'r') as f:
                creds_data = json.load(f)

            # Create client with credentials
            client = vision.ImageAnnotatorClient.from_service_account_json(self.google_credentials_path)

            # Prepare image
            image = vision.Image(content=image_data)

            # Perform document text detection (best for documents/prescriptions)
            response = client.document_text_detection(
                image=image,
                image_context={"language_hints": ["en", "hi"]}  # English and Hindi
            )

            if response.error.message:
                raise Exception(f"Google Vision API error: {response.error.message}")

            # Extract full text
            if response.full_text_annotation:
                text = response.full_text_annotation.text
                logger.info(f"Google Vision extracted {len(text)} characters")
                return text

            # Fallback to text annotations
            if response.text_annotations:
                text = response.text_annotations[0].description
                logger.info(f"Google Vision (fallback) extracted {len(text)} characters")
                return text

            raise Exception("No text detected in image")

        except ImportError:
            raise Exception(
                "Google Cloud Vision library not installed. "
                "Install with: pip install google-cloud-vision"
            )

    async def _ocrspace_api(self, image_data: bytes, image_format: str) -> str:
        """OCR.space API - Free tier available, good quality"""
        import aiohttp

        url = "https://api.ocr.space/parse/image"

        # Compress image for OCR.space (1MB limit on free tier)
        compressed_data = self._compress_image(image_data, max_size_kb=900)

        # OCR.space supports base64
        image_base64 = base64.b64encode(compressed_data).decode('utf-8')

        payload = {
            "apikey": self.ocrspace_api_key,
            "base64Image": f"data:image/{image_format};base64,{image_base64}",
            "language": "eng",
            "isOverlayRequired": False,
            "detectOrientation": True,
            "scale": True,
            "OCREngine": 2,  # Engine 2 is better for mixed content
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"OCR.space API error: {error_text}")

                result = await response.json()

                if result.get("IsErroredOnProcessing"):
                    error_msg = result.get("ErrorMessage", ["Unknown error"])[0]
                    raise Exception(f"OCR.space processing error: {error_msg}")

                if result.get("ParsedResults"):
                    parsed_text = result["ParsedResults"][0].get("ParsedText", "")
                    if parsed_text:
                        return parsed_text

                raise Exception("No text detected in image")


# Singleton instance
_ocr_service = None

def get_ocr_service() -> OCRService:
    """Get or create OCR service singleton"""
    global _ocr_service
    if _ocr_service is None:
        _ocr_service = OCRService()
    return _ocr_service
