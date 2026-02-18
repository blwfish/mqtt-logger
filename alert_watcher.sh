#!/bin/bash
# Watch mqtt-logger alerts file and fire macOS notifications.
# Run via launchd (see com.blw.mqtt-alert-watcher.plist) or manually.

ALERT_FILE="$(dirname "$0")/data/alerts.log"

# Create file if it doesn't exist
touch "$ALERT_FILE"

# Tail new lines and send each as a macOS notification
tail -f -n 0 "$ALERT_FILE" | while IFS= read -r line; do
    # Strip the ISO timestamp prefix for the notification body
    msg="${line#* }"
    osascript -e "display notification \"$msg\" with title \"MQTT Loop Detected\" sound name \"Sosumi\"" 2>/dev/null
done
