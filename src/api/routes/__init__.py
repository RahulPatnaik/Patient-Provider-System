"""
API Routes Package.

Exports all route modules for easy importing.
"""

from api.routes import provider, health, admin

__all__ = ["provider", "health", "admin"]
