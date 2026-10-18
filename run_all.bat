@echo off
REM One-command default run on Windows (native Kafka). Extra arguments are passed through, e.g.
REM   run_all.bat --set producer_rate=5000 --set dgim_report_k=1000
REM   run_all.bat --run-id my-test --set partitions=1 --set parallelism=1
setlocal
cd /d "%~dp0"
set "PY=%USERPROFILE%\.venvs\bda-a2\Scripts\python.exe"
if not exist "%PY%" (
  echo Python environment not found. Run:  powershell -ExecutionPolicy Bypass -File setup_windows.ps1
  exit /b 1
)
"%PY%" scripts\kafka_ctl.py start || exit /b 1
"%PY%" scripts\run_experiment.py --run-id default %*
if errorlevel 1 exit /b 1
"%PY%" scripts\analyze_results.py
echo.
echo Results: results\runs\default\results.jsonl  results\experiment_summary.csv  results\analysis.md
