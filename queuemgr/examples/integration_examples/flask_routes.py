"""
Flask routes for Queue Manager integration.

This module contains the Flask routes for the integration example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from flask import Flask, jsonify, request
from typing import Dict, Any

from .flask_api import QueueManagerWebAPI


def create_web_app() -> Flask:
    """
    Create Flask web application with Queue Manager integration.

    Returns:
        Flask application instance.
    """
    app = Flask(__name__)
    api = QueueManagerWebAPI()

    @app.route("/")
    def index() -> str:
        """Home page."""
        return """
        <html>
        <head><title>Queue Manager Web Interface</title></head>
        <body>
            <h1>Queue Manager Web Interface</h1>
            <p>Welcome to the Queue Manager web interface!</p>
            <ul>
                <li><a href="/api/status">Service Status</a></li>
                <li><a href="/api/jobs">List Jobs</a></li>
            </ul>
        </body>
        </html>
        """

    @app.route("/api/status", methods=["GET"])
    def api_status() -> Dict[str, Any]:
        """Get service status."""
        return jsonify(api.get_service_status())

    @app.route("/api/start", methods=["POST"])
    def api_start_service() -> Dict[str, Any]:
        """Start the service."""
        return jsonify(api.start_service())

    @app.route("/api/stop", methods=["POST"])
    def api_stop_service() -> Dict[str, Any]:
        """Stop the service."""
        return jsonify(api.stop_service())

    @app.route("/api/jobs", methods=["GET"])
    def api_jobs() -> Dict[str, Any]:
        """List jobs."""
        status_filter = request.args.get("status")
        jobs = api.get_jobs(status_filter)
        return jsonify({"jobs": jobs})

    @app.route("/api/jobs/<job_id>", methods=["GET"])
    def api_job_status(job_id: str) -> Dict[str, Any]:
        """Get job status."""
        return jsonify(api.get_job_status(job_id))

    @app.route("/api/jobs/<job_id>/start", methods=["POST"])
    def api_start_job(job_id: str) -> Dict[str, Any]:
        """Start a job."""
        return jsonify(api.start_job(job_id))

    @app.route("/api/jobs/<job_id>/stop", methods=["POST"])
    def api_stop_job(job_id: str) -> Dict[str, Any]:
        """Stop a job."""
        return jsonify(api.stop_job(job_id))

    @app.route("/api/jobs/<job_id>", methods=["DELETE"])
    def api_delete_job(job_id: str) -> Dict[str, Any]:
        """Delete a job."""
        return jsonify(api.delete_job(job_id))

    @app.route("/api/jobs", methods=["POST"])
    def api_add_job() -> Dict[str, Any]:
        """Add a new job."""
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        job_class_name = data.get("job_class")
        job_id = data.get("job_id")
        params = data.get("params", {})

        if not job_class_name or not job_id:
            return jsonify({"error": "job_class and job_id are required"}), 400

        return jsonify(api.add_job(job_class_name, job_id, params))

    return app


def main() -> None:
    """Main function to run the Flask application."""
    app = create_web_app()
    print("🚀 Starting Queue Manager Web Interface...")
    print("📱 Open http://localhost:5000 in your browser")
    app.run(host="0.0.0.0", port=5000, debug=True)
