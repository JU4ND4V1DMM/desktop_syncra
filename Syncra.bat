@echo off

:: ============================================
:: Update repository
:: ============================================

cd /d "%~dp0"

echo Fetching latest changes from Git...
git fetch --all
git reset --hard origin/master
echo Repository successfully updated.

:: ============================================
:: Run application
:: ============================================

echo Starting Syncra...

if exist ".venv\Scripts\python.exe" (
    start "" /B ".venv\Scripts\python.exe" main.py
) else (
    start "" /B python main.py
)

if %errorlevel% neq 0 (
    echo Error: Failed to start main.py
    pause
)

exit