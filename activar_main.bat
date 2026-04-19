@echo off
title Automatizador Git

:: 1. OBLIGAR al script a ejecutarse en la carpeta donde esta este archivo
cd /d "%~dp0"

echo === Iniciando Sincronizacion ===
git config --local safe.directory "%CD%"

:: 2. Detectar rama
for /f %%i in ('git branch --show-current') do set BRANCH=%%i

if "%BRANCH%"=="" (
    echo ERROR: No se pudo detectar la rama actual.
    pause
    exit /b
)

echo Rama detectada: %BRANCH%
echo Descargando posibles cambios de la nube...
git pull origin %BRANCH%

echo Preparando archivos...
git add .

:: 3. Verificar si realmente hay cambios usando un metodo mas seguro (porcelain)
set CHANGES=
for /f "delims=" %%i in ('git status --porcelain') do set CHANGES=%%i

if "%CHANGES%"=="" (
    echo.
    echo INFO: No hay cambios nuevos para guardar (working tree clean).
    echo Verificando si hay algo pendiente de subir a la nube...
    git push -u origin %BRANCH%
    echo === Proceso Terminado ===
    pause
    exit /b
)

:: 4. Pedir mensaje (con proteccion por si presionas Enter sin escribir nada)
echo.
set /p msg="Escribe que hiciste hoy (o presiona Enter para mensaje por defecto): "
if "%msg%"=="" set msg="Actualizacion de codigo automatica"

git commit -m "%msg%"

echo Subiendo a GitHub...
git push -u origin %BRANCH%

echo === Proceso Terminado ===
pause