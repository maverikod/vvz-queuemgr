"""
Flask Integration Example for Queue Manager.

This example demonstrates how to integrate Queue Manager into a Flask web application.
Shows how to create REST API endpoints for job management and monitoring.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all functionality from separate modules
from .flask_api import QueueManagerWebAPI
from .flask_routes import create_web_app, main

# Re-export for backward compatibility
__all__ = ["QueueManagerWebAPI", "create_web_app", "main"]
