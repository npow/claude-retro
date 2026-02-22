#!/bin/bash
set -euo pipefail

echo "Installing build dependencies..."
pip install pyinstaller pywebview

echo "Building macOS .app bundle..."
pyinstaller claude_retro.spec --clean

echo ""
echo "Done! Output: dist/Claude Retro.app"
