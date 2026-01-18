#!/usr/bin/env python3
"""
Start both backend API and frontend server.

Usage:
    python start_app.py
"""

import subprocess
import time
import webbrowser
import sys
from pathlib import Path

def start_backend():
    """Start the FastAPI backend server."""
    print("🚀 Starting FastAPI backend on http://localhost:8000")
    backend_process = subprocess.Popen(
        [sys.executable, "src/main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    return backend_process

def start_frontend():
    """Start the frontend HTTP server."""
    print("🚀 Starting frontend server on http://localhost:3000")
    frontend_process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "3000", "--directory", "frontend"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    return frontend_process

def main():
    print("=" * 80)
    print("KPME Provider Validation System")
    print("=" * 80)

    # Start backend
    backend_process = start_backend()
    time.sleep(3)  # Wait for backend to start

    # Start frontend
    frontend_process = start_frontend()
    time.sleep(2)  # Wait for frontend to start

    print("\n" + "=" * 80)
    print("✅ Application started successfully!")
    print("=" * 80)
    print("\n📊 Access Points:")
    print("  - Frontend:  http://localhost:3000")
    print("  - Backend API: http://localhost:8000")
    print("  - API Docs:  http://localhost:8000/docs")
    print("\n💡 Opening frontend in browser...")
    print("\nPress Ctrl+C to stop both servers")
    print("=" * 80 + "\n")

    # Open browser
    time.sleep(1)
    webbrowser.open("http://localhost:3000")

    try:
        # Keep running and show logs
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
        backend_process.terminate()
        frontend_process.terminate()
        backend_process.wait()
        frontend_process.wait()
        print("✅ Servers stopped")

if __name__ == "__main__":
    main()
