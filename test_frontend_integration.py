#!/usr/bin/env python3
"""
Comprehensive frontend-backend integration test.

This script tests all API endpoints that the frontend uses.
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"
FRONTEND_URL = "http://localhost:3000"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BLUE}{'=' * 80}{Colors.END}")
    print(f"{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BLUE}{'=' * 80}{Colors.END}\n")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.YELLOW}ℹ️  {text}{Colors.END}")

def test_backend_health():
    """Test if backend is running."""
    print_header("TEST 1: Backend Health Check")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print_success(f"Backend is running - Status: {data['status']}")
            print_info(f"Version: {data['version']}, Uptime: {data['uptime_seconds']:.2f}s")
            return True
        else:
            print_error(f"Backend returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print_error("Cannot connect to backend! Is it running?")
        print_info("Start backend: python src/main.py")
        return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_frontend_accessibility():
    """Test if frontend is accessible."""
    print_header("TEST 2: Frontend Accessibility")
    try:
        response = requests.get(FRONTEND_URL, timeout=5)
        if response.status_code == 200:
            print_success("Frontend is accessible")
            if "KPME" in response.text:
                print_success("Frontend contains expected content")
                return True
            else:
                print_error("Frontend content seems incorrect")
                return False
        else:
            print_error(f"Frontend returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print_error("Cannot connect to frontend! Is it running?")
        print_info("Start frontend: python -m http.server 3000 --directory frontend")
        return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_fast_validation():
    """Test fast validation endpoint."""
    print_header("TEST 3: Fast Certificate Validation")
    try:
        payload = {
            "certificate_number": "BDR00172ALHL2",
            "provider_name": "AAROGYA HOSPITAL",
            "use_cache": True
        }

        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/api/v1/providers/validate/fast",
            json=payload,
            timeout=10
        )
        elapsed_time = (time.time() - start_time) * 1000

        if response.status_code == 200:
            data = response.json()
            print_success(f"Fast validation successful - Time: {elapsed_time:.2f}ms")
            print_info(f"Certificate: {data['certificate_number']}")
            print_info(f"Valid: {data['is_valid']}, Confidence: {data['confidence']}")
            print_info(f"Cache Hit: {data['cache_hit']}, Execution Time: {data['execution_time_ms']}ms")
            return True
        else:
            print_error(f"Fast validation failed with status {response.status_code}")
            print_error(f"Response: {response.text}")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_full_validation():
    """Test full validation endpoint."""
    print_header("TEST 4: Full Provider Validation")
    try:
        payload = {
            "establishment_name": "AAROGYA HOSPITAL",
            "certificate_number": "BDR00172ALHL2",
            "phone": "9448452147",
            "district": "Bangalore Urban"
        }

        print_info("Sending validation request (may take a few seconds)...")
        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/api/v1/providers/validate",
            json=payload,
            timeout=30
        )
        elapsed_time = (time.time() - start_time) * 1000

        if response.status_code == 200:
            data = response.json()
            print_success(f"Full validation successful - Time: {elapsed_time:.2f}ms")
            print_info(f"Decision: {data['decision']}")
            print_info(f"Confidence: {data['final_confidence']:.2f} ({data['confidence_level']})")
            print_info(f"Validation Path: {data['validation_path']}")
            print_info(f"Execution Time: {data['execution_time_ms']}ms")

            if data.get('reasoning'):
                print_info(f"Reasoning: {', '.join(data['reasoning'][:3])}")

            return True
        else:
            print_error(f"Full validation failed with status {response.status_code}")
            print_error(f"Response: {response.text[:500]}")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_services_health():
    """Test services health endpoint."""
    print_header("TEST 5: Services Health Check")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/health/services", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print_success("Services health check successful")
            print_info(f"Database: {data['database']['status']} - {data['database'].get('total_establishments', 0)} establishments")
            print_info(f"Cache: {data['cache']['type']} - {data['cache']['status']}")
            print_info(f"Overall: {data['overall_status']}")
            return True
        else:
            print_error(f"Services health check failed with status {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_system_stats():
    """Test system statistics endpoint."""
    print_header("TEST 6: System Statistics")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/admin/stats", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print_success("System stats retrieved successfully")
            print_info(f"Total Establishments: {data['total_establishments']}")
            print_info(f"Total Staff: {data['total_staff']}")
            print_info(f"Total Districts: {data['total_districts']}")
            print_info(f"Total Validations: {data['total_validations']}")
            return True
        else:
            print_error(f"System stats failed with status {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_search():
    """Test search endpoint."""
    print_header("TEST 7: Establishment Search")
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/admin/search/establishments?query=AAROGYA&limit=5",
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            print_success(f"Search successful - Found {data['total_results']} results")
            if data['total_results'] > 0:
                first_result = data['results'][0]
                print_info(f"First result: {first_result.get('establishment_name', first_result.get('name'))}")
            return True
        else:
            print_error(f"Search failed with status {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def test_cors():
    """Test CORS headers."""
    print_header("TEST 8: CORS Configuration")
    try:
        response = requests.options(
            f"{BASE_URL}/api/v1/health",
            headers={
                'Origin': FRONTEND_URL,
                'Access-Control-Request-Method': 'GET'
            },
            timeout=5
        )

        if 'Access-Control-Allow-Origin' in response.headers:
            print_success("CORS headers present")
            print_info(f"Allow-Origin: {response.headers.get('Access-Control-Allow-Origin')}")
            return True
        else:
            print_error("CORS headers missing")
            return False
    except Exception as e:
        print_error(f"Error: {e}")
        return False

def main():
    """Run all tests."""
    print(f"\n{Colors.BLUE}")
    print("=" * 80)
    print("KPME Provider Validation System - Integration Test Suite")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(Colors.END)

    tests = [
        ("Backend Health", test_backend_health),
        ("Frontend Accessibility", test_frontend_accessibility),
        ("Fast Validation", test_fast_validation),
        ("Full Validation", test_full_validation),
        ("Services Health", test_services_health),
        ("System Statistics", test_system_stats),
        ("Search", test_search),
        ("CORS", test_cors)
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Test '{test_name}' crashed: {e}")
            results.append((test_name, False))
        time.sleep(0.5)  # Brief pause between tests

    # Summary
    print_header("TEST SUMMARY")
    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.END}" if result else f"{Colors.RED}FAIL{Colors.END}"
        print(f"  {test_name}: {status}")

    print(f"\n{Colors.BLUE}Results: {passed}/{total} tests passed{Colors.END}")

    if passed == total:
        print(f"\n{Colors.GREEN}🎉 ALL TESTS PASSED! System is working correctly.{Colors.END}")
        print(f"\n{Colors.YELLOW}Access the application at: {FRONTEND_URL}{Colors.END}")
    else:
        print(f"\n{Colors.RED}⚠️  Some tests failed. Please check the errors above.{Colors.END}")

    print(f"\n{Colors.BLUE}{'=' * 80}{Colors.END}\n")

if __name__ == "__main__":
    main()
