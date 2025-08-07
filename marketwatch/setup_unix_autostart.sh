#!/bin/bash
# Setup auto-start for MarketWatch NYSE Scheduler on macOS/Linux

echo "MarketWatch NYSE Scheduler - Unix Auto-Start Setup"
echo "=================================================="
echo ""

# Detect OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS_TYPE="macos"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS_TYPE="linux"
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Make the scheduler script executable
chmod +x "$SCRIPT_DIR/marketwatch_scheduler_unix.sh"

if [ "$OS_TYPE" == "macos" ]; then
    echo "Setting up for macOS using launchd..."
    echo ""
    
    # Create LaunchAgent plist
    PLIST_NAME="com.oosit.marketwatch-scheduler"
    PLIST_FILE="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"
    
    cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${SCRIPT_DIR}/marketwatch_scheduler_unix.sh</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>${HOME}/Library/Logs/${PLIST_NAME}.log</string>
    <key>StandardErrorPath</key>
    <string>${HOME}/Library/Logs/${PLIST_NAME}.error.log</string>
</dict>
</plist>
EOF
    
    echo "Created LaunchAgent at: $PLIST_FILE"
    echo ""
    echo "To enable auto-start:"
    echo "  launchctl load $PLIST_FILE"
    echo ""
    echo "To start now:"
    echo "  launchctl start $PLIST_NAME"
    echo ""
    echo "To disable auto-start:"
    echo "  launchctl unload $PLIST_FILE"
    echo ""
    echo "To check status:"
    echo "  launchctl list | grep $PLIST_NAME"
    echo ""
    echo "Logs are stored at:"
    echo "  ~/Library/Logs/${PLIST_NAME}.log"
    echo "  ~/Library/Logs/${PLIST_NAME}.error.log"
    
elif [ "$OS_TYPE" == "linux" ]; then
    echo "Setting up for Linux using systemd..."
    echo ""
    
    # Update the service file with correct paths
    SERVICE_FILE="$SCRIPT_DIR/marketwatch-scheduler.service"
    USER_SERVICE_DIR="$HOME/.config/systemd/user"
    
    # Create user systemd directory if it doesn't exist
    mkdir -p "$USER_SERVICE_DIR"
    
    # Create personalized service file
    cat > "$USER_SERVICE_DIR/marketwatch-scheduler.service" << EOF
[Unit]
Description=MarketWatch NYSE Scheduler
After=network.target

[Service]
Type=simple
WorkingDirectory=${SCRIPT_DIR}
ExecStart=/usr/bin/python3 ${SCRIPT_DIR}/marketwatch_nyse_scheduler.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment="PYTHONUNBUFFERED=1"
StartLimitInterval=60
StartLimitBurst=5

[Install]
WantedBy=default.target
EOF
    
    echo "Created systemd service at: $USER_SERVICE_DIR/marketwatch-scheduler.service"
    echo ""
    echo "To enable auto-start:"
    echo "  systemctl --user daemon-reload"
    echo "  systemctl --user enable marketwatch-scheduler.service"
    echo ""
    echo "To start now:"
    echo "  systemctl --user start marketwatch-scheduler.service"
    echo ""
    echo "To check status:"
    echo "  systemctl --user status marketwatch-scheduler.service"
    echo ""
    echo "To view logs:"
    echo "  journalctl --user -u marketwatch-scheduler.service -f"
    echo ""
    echo "To disable auto-start:"
    echo "  systemctl --user disable marketwatch-scheduler.service"
fi

echo ""
echo "Setup complete!"
echo ""
echo "Alternative: To run manually with auto-restart:"
echo "  ./marketwatch_scheduler_unix.sh"