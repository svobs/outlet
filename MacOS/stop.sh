#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
launchctl unload "$SCRIPT_DIR/com.msvoboda.outlet.agent.plist"
