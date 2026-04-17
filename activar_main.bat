@echo off
title Automatizador Git - %CD%
echo === Iniciando Sincronizacion ===

git config --local safe.directory "%CD%"

for /f %%i in ('git branch --show-current') do set BRANCH=%%i

if "%BRANCH%"=="" (
    echo No se pudo detectar la rama actual.
    pause
    exit /b
)

echo Rama detectada: %BRANCH%
echo Descargando posibles cambios de la nube...
git pull origin %BRANCH%

echo Preparando archivos...
git add .

git diff --cached --quiet
if %errorlevel%==0 (
    echo No hay cambios para guardar.
    echo Verificando push...
    git push -u origin %BRANCH%
    echo === Proceso Terminado ===
    pause
    exit /b
)

set /p msg="Escribe que hiciste hoy: "
git commit -m "%msg%"

echo Subiendo a GitHub...
git push -u origin %BRANCH%

echo === Proceso Terminado ===
pause