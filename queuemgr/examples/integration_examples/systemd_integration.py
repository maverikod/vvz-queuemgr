"""
Queue Manager Daemon Service.

This module provides a daemon service that runs the queue manager
in the background with proper signal handling and logging.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import os
import sys
import signal
import time
import logging
import argparse
import tempfile
from pathlib import Path
from typing import Optional

from queuemgr.proc_manager import ProcManager
from queuemgr.proc_config import ProcManagerConfig
from queuemgr.exceptions import ProcessControlError


class QueueManagerDaemon:
    """
    Daemon service for the Queue Manager.

    Provides a complete daemon implementation with signal handling,
    logging, and graceful shutdown.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the daemon.

        Args:
            config_path: Path to configuration file.
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.manager: Optional[ProcManager] = None
        self.logger = self._setup_logging()
        self.running = False

    def _load_config(self) -> dict:
        """Load configuration from file or use defaults."""
        if self.config_path and Path(self.config_path).exists():
            # TODO: Implement config file loading
            pass

        # Use temp directories for testing
        temp_dir = Path(tempfile.gettempdir()) / "queuemgr_test"

        return ProcManagerConfig(
            registry_path=str(temp_dir / "registry.jsonl"),
            proc_dir=str(temp_dir / "proc"),
            shutdown_timeout=30.0,
            cleanup_interval=300.0,  # 5 minutes
            max_concurrent_jobs=50,
        )

    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the daemon."""
        logger = logging.getLogger("queuemgr.daemon")
        logger.setLevel(logging.INFO)

        # Create logs directory
        log_dir = Path(tempfile.gettempdir()) / "queuemgr_test" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(log_dir / "daemon.log")
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def start(self) -> None:
        """Start the daemon service."""
        if self.running:
            self.logger.warning("Daemon is already running")
            return

        try:
            self.logger.info("Starting Queue Manager Daemon...")

            # Create necessary directories
            self._create_directories()

            # Setup signal handlers
            self._setup_signal_handlers()

            # Start the manager
            self.manager = ProcManager(self.config)
            self.manager.start()

            self.running = True
            self.logger.info("Queue Manager Daemon started successfully")

            # Main daemon loop
            self._daemon_loop()

        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self.logger.error(f"Failed to start daemon: {e}")
            raise

    def stop(self) -> None:
        """Stop the daemon service."""
        if not self.running:
            self.logger.warning("Daemon is not running")
            return

        try:
            self.logger.info("Stopping Queue Manager Daemon...")

            if self.manager:
                self.manager.stop()
                self.manager = None

            self.running = False
            self.logger.info("Queue Manager Daemon stopped successfully")

        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self.logger.error(f"Error stopping daemon: {e}")
            raise

    def restart(self) -> None:
        """Restart the daemon service."""
        self.logger.info("Restarting Queue Manager Daemon...")
        self.stop()
        time.sleep(2)
        self.start()

    def status(self) -> dict:
        """Get daemon status."""
        if not self.running or not self.manager:
            return {"status": "stopped", "manager_running": False, "uptime": 0}

        return {
            "status": "running",
            "manager_running": self.manager.is_running(),
            "uptime": (
                time.time() - self._start_time if hasattr(self, "_start_time") else 0
            ),
            "config": {
                "registry_path": self.config.registry_path,
                "proc_dir": self.config.proc_dir,
                "max_concurrent_jobs": self.config.max_concurrent_jobs,
            },
        }

    def _create_directories(self) -> None:
        """Create necessary directories."""
        directories = [
            Path(self.config.registry_path).parent,
            Path(self.config.proc_dir),
            Path("/var/log/queuemgr"),
            Path("/var/lib/queuemgr"),
            Path("/var/run/queuemgr"),
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Created directory: {directory}")

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            """
            Handle OS signals for graceful shutdown.

            Args:
                signum: Signal number.
                frame: Current stack frame.
            """
            self.logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)  # Reload config

    def _daemon_loop(self) -> None:
        """Main daemon loop."""
        self._start_time = time.time()

        try:
            while self.running:
                # Check manager health
                if self.manager and not self.manager.is_running():
                    self.logger.error("Manager process died, attempting restart...")
                    try:
                        self.manager.start()
                    except (
                        OSError,
                        IOError,
                        ValueError,
                        TimeoutError,
                        ProcessControlError,
                    ) as e:
                        self.logger.error(f"Failed to restart manager: {e}")
                        break

                # Sleep for a bit
                time.sleep(5)

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self.logger.error(f"Daemon loop error: {e}")
        finally:
            self.stop()


def main():
    """Main entry point for the daemon."""
    parser = argparse.ArgumentParser(description="Queue Manager Daemon")
    parser.add_argument(
        "command", choices=["start", "stop", "restart", "status"], help="Daemon command"
    )
    parser.add_argument("--config", "-c", help="Configuration file path")
    parser.add_argument(
        "--daemon", "-d", action="store_true", help="Run as daemon (fork to background)"
    )
    parser.add_argument(
        "--pidfile", "-p", default="/var/run/queuemgr/daemon.pid", help="PID file path"
    )

    args = parser.parse_args()

    daemon = QueueManagerDaemon(args.config)

    if args.command == "start":
        if args.daemon:
            # Fork to background
            pid = os.fork()
            if pid > 0:
                # Parent process
                sys.exit(0)
            elif pid == 0:
                # Child process
                os.setsid()
                os.chdir("/")
                os.umask(0)

                # Write PID file
                Path(args.pidfile).parent.mkdir(parents=True, exist_ok=True)
                with open(args.pidfile, "w") as f:
                    f.write(str(os.getpid()))

                daemon.start()
        else:
            daemon.start()

    elif args.command == "stop":
        daemon.stop()

    elif args.command == "restart":
        daemon.restart()

    elif args.command == "status":
        status = daemon.status()
        print(f"Status: {status['status']}")
        print(f"Manager running: {status['manager_running']}")
        print(f"Uptime: {status['uptime']:.1f} seconds")


if __name__ == "__main__":
    main()
