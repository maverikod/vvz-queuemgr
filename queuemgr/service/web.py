"""
Web Interface for Queue Manager Service.

This module provides a web interface for managing the queue system
with real-time monitoring and job management.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
from typing import Dict, Any, List

from ..proc_api import (
    get_proc_queue_system,
    start_proc_queue_system,
    stop_proc_queue_system,
)
from ..exceptions import ProcessControlError


class QueueManagerWebAPI:
    """
    Web API for Queue Manager.

    Provides REST API endpoints for managing jobs and monitoring
    the queue system.
    """

    def __init__(self):
        """Initialize the web API."""
        self.queue = None

    def get_service_status(self) -> Dict[str, Any]:
        """Get service status."""
        try:
            queue = get_proc_queue_system()
            if queue.is_running():
                jobs = queue.list_jobs()
                running_jobs = [job for job in jobs if job.get("status") == "RUNNING"]

                return {
                    "status": "running",
                    "total_jobs": len(jobs),
                    "running_jobs": len(running_jobs),
                    "uptime": time.time(),  # Simplified
                }
            else:
                return {"status": "stopped"}

        except ProcessControlError:
            return {"status": "error", "message": "Service not available"}

    def start_service(self) -> Dict[str, Any]:
        """Start the service."""
        try:
            start_proc_queue_system()
            return {"status": "success", "message": "Service started"}
        except ProcessControlError as e:
            return {"status": "error", "message": str(e)}

    def stop_service(self) -> Dict[str, Any]:
        """Stop the service."""
        try:
            stop_proc_queue_system()
            return {"status": "success", "message": "Service stopped"}
        except ProcessControlError as e:
            return {"status": "error", "message": str(e)}

    def get_jobs(self, status_filter: str | None = None) -> List[Dict[str, Any]]:
        """Get all jobs."""
        try:
            queue = get_proc_queue_system()
            jobs = queue.list_jobs()

            if status_filter:
                jobs = [job for job in jobs if job.get("status") == status_filter]

            return jobs

        except ProcessControlError:
            return []

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status."""
        try:
            queue = get_proc_queue_system()
            return queue.get_job_status(job_id)
        except ProcessControlError as e:
            return {"error": str(e)}

    def start_job(self, job_id: str) -> Dict[str, Any]:
        """Start a job."""
        try:
            queue = get_proc_queue_system()
            queue.start_job(job_id)
            return {"status": "success", "message": f"Job {job_id} started"}
        except ProcessControlError as e:
            return {"status": "error", "message": str(e)}

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        """Stop a job."""
        try:
            queue = get_proc_queue_system()
            queue.stop_job(job_id)
            return {"status": "success", "message": f"Job {job_id} stopped"}
        except ProcessControlError as e:
            return {"status": "error", "message": str(e)}

    def delete_job(self, job_id: str, force: bool = False) -> Dict[str, Any]:
        """Delete a job."""
        try:
            queue = get_proc_queue_system()
            queue.delete_job(job_id, force)
            return {"status": "success", "message": f"Job {job_id} deleted"}
        except ProcessControlError as e:
            return {"status": "error", "message": str(e)}

    def add_job(
        self, job_class_name: str, job_id: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add a job."""
        try:
            queue = get_proc_queue_system()

            # Import job class (simplified)
            job_class = self._import_job_class(job_class_name)

            queue.add_job(job_class, job_id, params)
            return {"status": "success", "message": f"Job {job_id} added"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _import_job_class(self, job_class_name: str):
        """Import job class by name."""
        from ..jobs.base import QueueJobBase

        # For demo purposes, return a simple job class
        class SimpleJob(QueueJobBase):
            def __init__(self, job_id: str, params: dict):
                super().__init__(job_id, params)

            def execute(self) -> None:
                print(f"SimpleJob {self.job_id} executed")

            def on_start(self) -> None:
                print(f"SimpleJob {self.job_id} started")

            def on_stop(self) -> None:
                print(f"SimpleJob {self.job_id} stopped")

            def on_end(self) -> None:
                print(f"SimpleJob {self.job_id} ended")

            def on_error(self, exc: BaseException) -> None:
                print(f"SimpleJob {self.job_id} error: {exc}")

        return SimpleJob


def create_web_app():
    """Create Flask web application."""
    try:
        from flask import Flask, jsonify, request, render_template_string
    except ImportError:
        raise ImportError(
            "Flask is required for web interface. Install with: pip install flask"
        )

    app = Flask(__name__)
    api = QueueManagerWebAPI()

    # HTML template for the web interface
    HTML_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Queue Manager</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .header { background: #f0f0f0; padding: 20px; border-radius: 5px; }
            .section { margin: 20px 0; }
            .job { border: 1px solid #ddd; padding: 10px; margin: 5px 0; border-radius: 3px; }
            .status-running { background: #e8f5e8; }
            .status-completed { background: #e8f5e8; }
            .status-error { background: #ffe8e8; }
            .status-pending { background: #fff8e8; }
            button { padding: 5px 10px; margin: 2px; cursor: pointer; }
            .btn-start { background: #4CAF50; color: white; border: none; }
            .btn-stop { background: #f44336; color: white; border: none; }
            .btn-delete { background: #ff9800; color: white; border: none; }
            .refresh-btn { background: #2196F3; color: white; border: none; padding: 10px 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Queue Manager</h1>
            <p>Real-time job monitoring and management</p>
            <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>
        </div>

        <div class="section">
            <h2>📊 Service Status</h2>
            <div id="service-status">Loading...</div>
        </div>

        <div class="section">
            <h2>📋 Jobs</h2>
            <div id="jobs-list">Loading...</div>
        </div>

        <div class="section">
            <h2>➕ Add Job</h2>
            <form onsubmit="addJob(event)">
                <input type="text" id="job-id" placeholder="Job ID" required>
                <input type="text" id="job-params" placeholder='{"param": "value"}' value='{"duration": 5}'>
                <button type="submit">Add Job</button>
            </form>
        </div>

        <script>
            function updateStatus() {
                fetch('/api/status')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('service-status').innerHTML =
                            `<p><strong>Status:</strong> ${data.status}</p>
                             <p><strong>Total Jobs:</strong> ${data.total_jobs || 0}</p>
                             <p><strong>Running Jobs:</strong> ${data.running_jobs || 0}</p>`;
                    });

                fetch('/api/jobs')
                    .then(response => response.json())
                    .then(jobs => {
                        let html = '';
                        jobs.forEach(job => {
                            const statusClass = `status-${job.status.toLowerCase()}`;
                            html += `
                                <div class="job ${statusClass}">
                                    <strong>${job.job_id}</strong> - ${job.status}
                                    <br>Progress: ${job.progress || 0}%
                                    <br>Description: ${job.description || 'No description'}
                                    <br>
                                    <button class="btn-start" onclick="startJob('${job.job_id}')">Start</button>
                                    <button class="btn-stop" onclick="stopJob('${job.job_id}')">Stop</button>
                                    <button class="btn-delete" onclick="deleteJob('${job.job_id}')">Delete</button>
                                </div>
                            `;
                        });
                        document.getElementById('jobs-list').innerHTML = html || '<p>No jobs found</p>';
                    });
            }

            function startJob(jobId) {
                fetch(`/api/jobs/${jobId}/start`, {method: 'POST'})
                    .then(response => response.json())
                    .then(data => {
                        alert(data.message);
                        updateStatus();
                    });
            }

            function stopJob(jobId) {
                fetch(`/api/jobs/${jobId}/stop`, {method: 'POST'})
                    .then(response => response.json())
                    .then(data => {
                        alert(data.message);
                        updateStatus();
                    });
            }

            function deleteJob(jobId) {
                if (confirm('Are you sure you want to delete this job?')) {
                    fetch(`/api/jobs/${jobId}/delete`, {method: 'POST'})
                        .then(response => response.json())
                        .then(data => {
                            alert(data.message);
                            updateStatus();
                        });
                }
            }

            function addJob(event) {
                event.preventDefault();
                const jobId = document.getElementById('job-id').value;
                const params = document.getElementById('job-params').value;

                try {
                    const paramsObj = JSON.parse(params);
                    fetch('/api/jobs', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            job_class_name: 'SimpleJob',
                            job_id: jobId,
                            params: paramsObj
                        })
                    })
                    .then(response => response.json())
                    .then(data => {
                        alert(data.message);
                        updateStatus();
                    });
                } catch (e) {
                    alert('Invalid JSON parameters');
                }
            }

            // Auto-refresh every 5 seconds
            setInterval(updateStatus, 5000);
            updateStatus();
        </script>
    </body>
    </html>
    """

    @app.route("/")
    def index():
        """Main page."""
        return render_template_string(HTML_TEMPLATE)

    @app.route("/api/status")
    def api_status():
        """Get service status."""
        return jsonify(api.get_service_status())

    @app.route("/api/jobs")
    def api_jobs():
        """Get all jobs."""
        return jsonify(api.get_jobs())

    @app.route("/api/jobs/<job_id>")
    def api_job_status(job_id):
        """Get job status."""
        return jsonify(api.get_job_status(job_id))

    @app.route("/api/jobs/<job_id>/start", methods=["POST"])
    def api_start_job(job_id):
        """Start a job."""
        return jsonify(api.start_job(job_id))

    @app.route("/api/jobs/<job_id>/stop", methods=["POST"])
    def api_stop_job(job_id):
        """Stop a job."""
        return jsonify(api.stop_job(job_id))

    @app.route("/api/jobs/<job_id>/delete", methods=["POST"])
    def api_delete_job(job_id):
        """Delete a job."""
        return jsonify(api.delete_job(job_id))

    @app.route("/api/jobs", methods=["POST"])
    def api_add_job():
        """Add a job."""
        data = request.get_json()
        return jsonify(
            api.add_job(data["job_class_name"], data["job_id"], data["params"])
        )

    return app


def main():
    """Main entry point for web interface."""
    import argparse

    parser = argparse.ArgumentParser(description="Queue Manager Web Interface")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    app = create_web_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
