#!/bin/bash
# Watch mqtt-logger alerts file and fire macOS notifications.
# Run via launchd (see com.blw.mqtt-alert-watcher.plist) or manually.

ALERT_FILE="$(dirname "$0")/data/alerts.log"

# Create file if it doesn't exist
touch "$ALERT_FILE"

# AppleScript template that reads the message body from argv, so the
# (attacker-controlled) MQTT topic embedded in $msg can never break out
# of the string literal — see https://en.wikipedia.org/wiki/AppleScript
APPLESCRIPT='on run argv
  display notification (item 1 of argv) with title (item 2 of argv) sound name "Sosumi"
end run'

# Tail new lines and send each as a macOS notification
tail -f -n 0 "$ALERT_FILE" | while IFS= read -r line; do
    # Strip the ISO timestamp prefix for the notification body
    msg="${line#* }"
    osascript -e "$APPLESCRIPT" -- "$msg" "MQTT Loop Detected" 2>/dev/null
done
