@echo off
:: ============================================
:: Script para sincronizar repositorio correcto
:: ============================================

:: 🧭 Mover al directorio del script
cd /d "%~dp0"

:: 📂 Definir variables
set "REPO_URL=https://github.com/Recupera-Sas/devops_syncra.git"
set "REPO_NAME=devops_syncra"
set "TEMP_DIR=%~dp0%REPO_NAME%_temp"

echo 🔄 Sincronizando con el repositorio oficial...

:: 🔍 Verificar si ya es un repositorio Git
if exist ".git" (
    echo 📦 Directorio ya es un repositorio Git...
    
    :: Verificar si el origen es correcto
    git remote get-url origin >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('git remote get-url origin') do set "CURRENT_URL=%%i"
        
        if not "%CURRENT_URL%"=="%REPO_URL%" (
            echo 🔄 Cambiando origen a repositorio correcto...
            git remote set-url origin %REPO_URL%
            git remote set-url --push origin %REPO_URL%
        )
    ) else (
        echo ➕ Configurando origen...
        git remote add origin %REPO_URL%
    )
    
    echo 📥 Actualizando desde remoto...
    git fetch --all
    git reset --hard origin/master
    git clean -fd
) else (
    echo 📦 Clonando repositorio por primera vez...
    git clone %REPO_URL% "%TEMP_DIR%"
    
    :: Copiar contenido al directorio actual
    xcopy "%TEMP_DIR%\*" "." /E /Y /Q
    rmdir /S /Q "%TEMP_DIR%"
)

echo ✅ Repositorio sincronizado correctamente.

:: ============================================
:: Ejecutar Python
:: ============================================

set "SUPPORT_ENV=%USERPROFILE%\.virtualenvs\soporteit-hFdpDLPc\Scripts\python.exe"

echo 🚀 Ejecutando main.py...

if exist "%SUPPORT_ENV%" (
    echo 🐍 Usando entorno soporteit...
    start "" /B "%SUPPORT_ENV%" main.py
) else (
    echo ⚠️ Usando Python del sistema...
    start "" /B python main.py
)

exit
