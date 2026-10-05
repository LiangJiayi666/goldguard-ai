@echo off
REM Clean history runs: interactive menu, or pass args directly.
cd /d "%~dp0"
if not "%1"=="" goto :run
:menu
echo.
echo  ====== Clean History ======
echo  1. Keep each company's latest batch, delete all older batches.
echo  2. Delete all batches before a given date/time.
echo.
set MODE=
set /p MODE=Choose 1 or 2: 
if "%MODE%"=="1" goto :mode1
if "%MODE%"=="2" goto :mode2
echo Invalid choice, try again.
goto :menu
:mode1
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\clean_history.ps1"
goto :end
:mode2
set BEFORE=
set /p BEFORE=Enter cutoff YYYYMMDD (e.g. 20260801): 
if "%BEFORE%"=="" goto :nodate
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\clean_history.ps1" -Before %BEFORE%
goto :end
:nodate
echo No date given, abort.
goto :end
:run
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\clean_history.ps1" %*
:end
echo.
pause
