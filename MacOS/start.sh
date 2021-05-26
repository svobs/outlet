#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
launchctl load "$SCRIPT_DIR/com.msvoboda.outlet.agent.plist" && tail -f "$SCRIPT_DIR/../log/outlet_console.log"
