#!/bin/bash
# Queue Manager Installation Script

set -e

echo "🚀 Installing Queue Manager Service..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run as root (use sudo)"
    exit 1
fi

# Configuration
SERVICE_USER="queuemgr"
INSTALL_DIR="/opt/queuemgr"
VENV_DIR="$INSTALL_DIR/.venv"
SERVICE_FILE="/etc/systemd/system/queuemgr.service"

# Create user if doesn't exist
if ! id "$SERVICE_USER" &>/dev/null; then
    echo "👤 Creating user $SERVICE_USER..."
    useradd -r -s /bin/false -d "$INSTALL_DIR" "$SERVICE_USER"
fi

# Create directories
echo "📁 Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "/var/lib/queuemgr"
mkdir -p "/var/log/queuemgr"
mkdir -p "/var/run/queuemgr"

# Copy application files
echo "📦 Copying application files..."
cp -r . "$INSTALL_DIR/"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

# Create virtual environment
echo "🐍 Creating virtual environment..."
cd "$INSTALL_DIR"
python3 -m venv "$VENV_DIR"

# Install dependencies
echo "📚 Installing dependencies..."
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -e .

# Install systemd service
echo "⚙️ Installing systemd service..."
cp queuemgr.service "$SERVICE_FILE"
systemctl daemon-reload

# Set permissions
echo "🔐 Setting permissions..."
chown -R "$SERVICE_USER:$SERVICE_USER" "/var/lib/queuemgr"
chown -R "$SERVICE_USER:$SERVICE_USER" "/var/log/queuemgr"
chown -R "$SERVICE_USER:$SERVICE_USER" "/var/run/queuemgr"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

# Enable and start service
echo "🔄 Enabling and starting service..."
systemctl enable queuemgr
systemctl start queuemgr

# Check status
echo "📊 Checking service status..."
if systemctl is-active --quiet queuemgr; then
    echo "✅ Queue Manager service is running!"
    echo ""
    echo "🎉 Installation completed successfully!"
    echo ""
    echo "📋 Useful commands:"
    echo "  sudo systemctl status queuemgr    # Check service status"
    echo "  sudo systemctl stop queuemgr      # Stop service"
    echo "  sudo systemctl start queuemgr     # Start service"
    echo "  sudo systemctl restart queuemgr   # Restart service"
    echo "  sudo journalctl -u queuemgr -f    # View logs"
    echo ""
    echo "🌐 Web interface: http://localhost:5000"
    echo "💻 CLI: $VENV_DIR/bin/python -m queuemgr.service.cli"
else
    echo "❌ Service failed to start"
    echo "📋 Check logs: sudo journalctl -u queuemgr -f"
    exit 1
fi
