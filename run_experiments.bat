@echo off
REM Runs every required experiment (P1-P4, K1-K2, low/medium/high rate, restart, Bloom FP demo)
REM and regenerates results\analysis.md + results\plots. Takes ~40 minutes.
REM   run_experiments.bat --only P1,P2
setlocal
cd /d "%~dp0"
set "PY=%USERPROFILE%\.venvs\bda-a2\Scripts\python.exe"
"%PY%" scripts\kafka_ctl.py start || exit /b 1
"%PY%" scripts\run_suite.py %*
"%PY%" scripts\analyze_results.py
