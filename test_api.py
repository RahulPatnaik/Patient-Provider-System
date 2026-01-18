"""
Quick API test script to verify all endpoints work correctly.

Usage:
1. Start the API server: python src/main.py
2. Run this test script: python test_api.py
"""

import requests
import json
from datetime import datetime


BASE_URL = "http://localhost:8000"


def test_root():
    """Test root endpoint."""
    print("\n" + "=" * 80)
    print("TEST 1: Root Endpoint")
    print("=" * 80)

    response = requests.get(f"{BASE_URL}/")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    assert response.status_code == 200
    assert "name" in response.json()
    print("✅ Root endpoint working")


def test_health_check():
    """Test health check endpoint."""
    print("\n" + "=" * 80)
    print("TEST 2: Health Check")
    print("=" * 80)

    response = requests.get(f"{BASE_URL}/api/v1/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    print("✅ Health check working")


def test_services_health():
    """Test services health endpoint."""
    print("\n" + "=" * 80)
    print("TEST 3: Services Health")
    print("=" * 80)

    response = requests.get(f"{BASE_URL}/api/v1/health/services")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    assert response.status_code == 200
    assert "database" in response.json()
    assert "cache" in response.json()
    print("✅ Services health working")


def test_system_stats():
    """Test system stats endpoint."""
    print("\n" + "=" * 80)
    print("TEST 4: System Stats")
    print("=" * 80)

    response = requests.get(f"{BASE_URL}/api/v1/admin/stats")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    assert response.status_code == 200
    assert response.json()["total_establishments"] == 1000
    assert response.json()["total_staff"] == 4483
    print("✅ System stats working")


def test_fast_validate():
    """Test fast validation endpoint."""
    print("\n" + "=" * 80)
    print("TEST 5: Fast Certificate Validation")
    print("=" * 80)

    payload = {
        "certificate_number": "BDR00172ALHL2",
        "provider_name": "AAROGYA HOSPITAL",
        "use_cache": True
    }

    response = requests.post(f"{BASE_URL}/api/v1/providers/validate/fast", json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")

    assert response.status_code == 200
    assert response.json()["is_valid"] == True
    assert response.json()["certificate_number"] == "BDR00172ALHL2"
    print("✅ Fast validation working")


def test_validate_provider():
    """Test full provider validation endpoint."""
    print("\n" + "=" * 80)
    print("TEST 6: Full Provider Validation")
    print("=" * 80)

    payload = {
        "establishment_name": "AAROGYA HOSPITAL",
        "certificate_number": "BDR00172ALHL2",
        "phone": "9448452147",
        "district": "Bangalore Urban",
        "use_cache": True
    }

    response = requests.post(f"{BASE_URL}/api/v1/providers/validate", json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response Summary:")
    data = response.json()
    print(f"  - Decision: {data.get('decision')}")
    print(f"  - Confidence: {data.get('final_confidence')}")
    print(f"  - Path: {data.get('validation_path')}")
    print(f"  - Execution Time: {data.get('execution_time_ms')}ms")

    assert response.status_code == 200
    assert "decision" in data
    assert "final_confidence" in data
    print("✅ Full validation working")


def test_search_establishments():
    """Test establishment search endpoint."""
    print("\n" + "=" * 80)
    print("TEST 7: Search Establishments")
    print("=" * 80)

    response = requests.get(f"{BASE_URL}/api/v1/admin/search/establishments?query=AAROGYA&limit=5")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Query: {data.get('query')}")
    print(f"Total Results: {data.get('total_results')}")

    assert response.status_code == 200
    assert data.get("total_results", 0) > 0
    print("✅ Search working")


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("KPME Provider Validation API - Test Suite")
    print(f"Testing: {BASE_URL}")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 80)

    try:
        test_root()
        test_health_check()
        test_services_health()
        test_system_stats()
        test_fast_validate()
        test_validate_provider()
        test_search_establishments()

        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {str(e)}")
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Cannot connect to API server.")
        print("Make sure the API is running: python src/main.py")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {str(e)}")


if __name__ == "__main__":
    main()
