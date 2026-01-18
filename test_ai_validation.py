#!/usr/bin/env python3
"""
Comprehensive AI Validation Test
Verifies that AI (Mistral) is being used properly in validation
"""

import asyncio
import json
from datetime import datetime
from src.agents.data_validator import DataValidatorAgent
from src.database.kpme_db import get_kpme_db
from src.cache.factory import get_cache_instance

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    END = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BLUE}{'=' * 80}{Colors.END}")
    print(f"{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BLUE}{'=' * 80}{Colors.END}\n")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.PURPLE}ℹ️  {text}{Colors.END}")

async def test_ai_validation():
    """Test that AI agent is being used in validation"""

    print_header("KPME AI VALIDATION VERIFICATION TEST")
    print(f"Test Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Initialize components
    print_info("Initializing components...")
    db = get_kpme_db()
    cache = get_cache_instance()

    print_success(f"Database loaded: {db.get_stats()['total_establishments']} establishments")
    print_success(f"Cache initialized: {cache.__class__.__name__}")

    # Test 1: Data Validator with AI
    print_header("TEST 1: Data Validator Agent (WITH AI)")

    validator = DataValidatorAgent()

    provider_data = {
        "establishment_name": "AAROGYA HOSPITAL",
        "certificate_number": "BDR00172ALHL2",
        "phone": "9448452147",
        "district": "Bangalore Urban"
    }

    print_info("Running validation with ALL agents (Web Scraper, Enrichment, Compliance)...")
    print_info("This should trigger AI synthesis...")

    result = await validator.validate(
        provider_data=provider_data,
        use_web_scraper=True,
        use_enrichment=True,
        use_compliance=True
    )

    # Check if result has AI synthesis
    print("\n" + "=" * 80)
    print("VALIDATION RESULT:")
    print("=" * 80)
    print(f"Decision: {result.output.decision}")
    print(f"Confidence: {result.output.final_confidence}")
    print(f"Validation Path: {result.output.validation_path}")
    print(f"Execution Time: {result.output.execution_time_ms}ms")
    print(f"Data Sources Used: {result.output.provider_data.data_sources}")

    # Verify AI was used
    if result.output.validation_path == "complex":
        print_success("✓ Complex validation path used (AI synthesis)")
    else:
        print_warning(f"! Validation path: {result.output.validation_path}")

    if len(result.output.provider_data.data_sources) >= 2:
        print_success(f"✓ Multiple data sources synthesized: {result.output.provider_data.data_sources}")
    else:
        print_warning(f"! Only {len(result.output.provider_data.data_sources)} data source(s)")

    if result.output.reasoning and len(result.output.reasoning) > 0:
        print_success(f"✓ AI-generated reasoning provided:")
        for reason in result.output.reasoning:
            print(f"  - {reason}")
    else:
        print_error("✗ No reasoning provided (AI may not have been called)")

    # Test 2: Check individual agents
    print_header("TEST 2: Individual Agent Verification")

    # KPME Data
    if result.output.provider_data.kpme_data:
        print_success("✓ KPME Database agent working")
        print(f"  - Certificate: {result.output.provider_data.kpme_data.certificate_number}")
        print(f"  - Valid: {result.output.provider_data.kpme_data.is_valid}")
        print(f"  - Confidence: {result.output.provider_data.kpme_data.confidence}")
    else:
        print_error("✗ KPME Database agent failed")

    # Web Scraper
    if result.output.provider_data.web_scraper_data:
        print_success("✓ Web Scraper agent working")
        print(f"  - Found: {result.output.provider_data.web_scraper_data.found_data}")
        print(f"  - Fields enriched: {result.output.provider_data.web_scraper_data.fields_count}")
    else:
        print_error("✗ Web Scraper agent failed")

    # Enrichment
    if result.output.provider_data.enrichment_data:
        print_success("✓ Enrichment agent working")
        print(f"  - Staff count: {result.output.provider_data.enrichment_data.staff_count}")
        print(f"  - Certificates count: {result.output.provider_data.enrichment_data.certificates_count}")
    else:
        print_error("✗ Enrichment agent failed")

    # Compliance
    if result.output.provider_data.compliance_data:
        print_success("✓ Compliance agent working")
        print(f"  - Decision: {result.output.provider_data.compliance_data.decision}")
        print(f"  - Validity: {result.output.provider_data.compliance_data.regulatory_validity}")
    else:
        print_error("✗ Compliance agent failed")

    # Test 3: Verify AI Model Details
    print_header("TEST 3: AI Model Configuration")

    print_info("Checking Mistral AI configuration...")

    # Check if validator has model info
    if hasattr(validator, 'agent'):
        print_success(f"✓ Pydantic AI agent initialized: {validator.agent.__class__.__name__}")
        if hasattr(validator.agent, 'model'):
            print_success(f"✓ AI Model: {validator.agent.model}")
        else:
            print_info("Model info not directly accessible")
    else:
        print_warning("Agent attribute not found")

    # Test 4: Performance Metrics
    print_header("TEST 4: Performance Metrics")

    print(f"Total Execution Time: {result.output.execution_time_ms}ms")

    if result.output.execution_time_ms > 1000:
        print_success("✓ Execution time indicates AI processing (>1s)")
        print_info("  (Deterministic validation would be <100ms)")
    else:
        print_warning("! Fast execution time - AI may not have been used")

    # Test 5: Confidence Scoring
    print_header("TEST 5: Confidence Scoring Analysis")

    print(f"KPME Confidence: {result.output.provider_data.kpme_confidence}")
    print(f"Data Quality Confidence: {result.output.provider_data.data_quality_confidence}")
    print(f"Compliance Confidence: {result.output.provider_data.compliance_confidence}")
    print(f"Overall Confidence: {result.output.provider_data.overall_confidence}")
    print(f"Final Confidence: {result.output.final_confidence}")

    if result.output.final_confidence != result.output.provider_data.overall_confidence:
        print_success("✓ AI synthesis adjusted confidence score")
        print(f"  - Agent Average: {result.output.provider_data.overall_confidence}")
        print(f"  - AI Final: {result.output.final_confidence}")
    else:
        print_info("Confidence scores match (may indicate simple averaging)")

    # Summary
    print_header("TEST SUMMARY")

    ai_indicators = 0
    total_checks = 5

    # Check 1: Complex path
    if result.output.validation_path == "complex":
        ai_indicators += 1
        print_success("1. Complex validation path: YES")
    else:
        print_error("1. Complex validation path: NO")

    # Check 2: Multiple sources
    if len(result.output.provider_data.data_sources) >= 2:
        ai_indicators += 1
        print_success("2. Multiple data sources: YES")
    else:
        print_error("2. Multiple data sources: NO")

    # Check 3: Reasoning
    if result.output.reasoning and len(result.output.reasoning) > 0:
        ai_indicators += 1
        print_success("3. AI reasoning provided: YES")
    else:
        print_error("3. AI reasoning provided: NO")

    # Check 4: Execution time
    if result.output.execution_time_ms > 1000:
        ai_indicators += 1
        print_success("4. AI processing time: YES")
    else:
        print_error("4. AI processing time: NO")

    # Check 5: All agents working
    if (result.output.provider_data.kpme_data and
        result.output.provider_data.web_scraper_data and
        result.output.provider_data.enrichment_data and
        result.output.provider_data.compliance_data):
        ai_indicators += 1
        print_success("5. All agents operational: YES")
    else:
        print_error("5. All agents operational: NO")

    print("\n" + "=" * 80)
    print(f"AI USAGE INDICATORS: {ai_indicators}/{total_checks}")
    print("=" * 80)

    if ai_indicators >= 4:
        print_success("\n🎉 AI AGENT IS WORKING CORRECTLY!")
        print_success("Mistral AI is being used for multi-source synthesis")
    elif ai_indicators >= 2:
        print_warning("\n⚠️  AI AGENT MAY BE WORKING")
        print_warning("Some indicators present, but verification needed")
    else:
        print_error("\n❌ AI AGENT NOT WORKING PROPERLY")
        print_error("AI synthesis may not be occurring")

    return ai_indicators >= 4

if __name__ == "__main__":
    success = asyncio.run(test_ai_validation())
    exit(0 if success else 1)
