@echo off
:: ============================================
:: Update the project and run the Python script
:: ============================================

:: 🧭 Move to the directory where this script is located
cd /d "%~dp0"

echo 🔄 Fetching latest changes from Git...
git fetch --all
git reset --hard origin/master
echo ✅ Repository successfully updated.

:: ============================================
:: Run main.py using system Python
:: ============================================

echo 🚀 Running main.py...

:: Using start /B to run in the background without opening a new window
start "" /B python main.py

if %errorlevel% neq 0 (
    echo ❌ Error: Failed to start main.py. Please check if Python is in your PATH.
    pause
)

exit