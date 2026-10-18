# One-time setup on Windows without Docker/WSL/admin rights (what the reference machine used).
#   powershell -ExecutionPolicy Bypass -File final_consumer\setup_windows.ps1
# Installs: Python 3.12 venv + packages, Kafka 3.9.1, Hadoop winutils (+ the MSVC 2010 runtime DLL it needs).
# Java 17 is required:  winget install EclipseAdoptium.Temurin.17.JDK
$ErrorActionPreference = 'Stop'
$tools = "$env:USERPROFILE\tools"
New-Item -ItemType Directory -Force $tools | Out-Null

if (-not (Get-Command java -ErrorAction SilentlyContinue) -and -not (Test-Path 'C:\Program Files\Eclipse Adoptium')) {
    Write-Host 'Installing Java 17 (Temurin)...'
    winget install --id EclipseAdoptium.Temurin.17.JDK -e --accept-package-agreements --accept-source-agreements --silent
}
if (-not (py -0 | Select-String '3.12')) {
    winget install --id Python.Python.3.12 -e --scope user --accept-package-agreements --accept-source-agreements --silent
}

$venv = "$env:USERPROFILE\.venvs\bda-a2"
if (-not (Test-Path "$venv\Scripts\python.exe")) { py -3.12 -m venv $venv }
& "$venv\Scripts\python.exe" -m pip install --upgrade pip -q
& "$venv\Scripts\python.exe" -m pip install -r "$PSScriptRoot\..\requirements.txt" -q

if (-not (Test-Path "$tools\kafka\libs")) {
    Write-Host 'Downloading Kafka 3.9.1...'
    Invoke-WebRequest https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz -OutFile "$tools\kafka.tgz"
    tar -xzf "$tools\kafka.tgz" -C $tools
    Rename-Item "$tools\kafka_2.13-3.9.1" kafka
    Remove-Item "$tools\kafka.tgz"
}

$hb = "$tools\hadoop\bin"
New-Item -ItemType Directory -Force $hb | Out-Null
foreach ($f in 'winutils.exe', 'hadoop.dll') {
    if (-not (Test-Path "$hb\$f")) {
        Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/$f" -OutFile "$hb\$f"
    }
}
# winutils.exe links against MSVCR100.dll (Visual C++ 2010). If it is not installed system-wide,
# extract the Microsoft-signed DLL from the official redistributable without running its installer.
if (-not (Test-Path 'C:\Windows\System32\msvcr100.dll') -and -not (Test-Path "$hb\msvcr100.dll")) {
    $d = "$env:TEMP\vcr2010"
    New-Item -ItemType Directory -Force "$d\x", "$d\cab" | Out-Null
    Invoke-WebRequest https://download.microsoft.com/download/1/6/5/165255E7-1014-4D0A-B094-B6A430A6BFFC/vcredist_x64.exe -OutFile "$d\vcredist_x64.exe"
    tar -xf "$d\vcredist_x64.exe" -C "$d\x"
    expand.exe "$d\x\vc_red.cab" -F:* "$d\cab" | Out-Null
    Copy-Item "$d\cab\F_CENTRAL_msvcr100_x64" "$hb\msvcr100.dll"
}
& "$hb\winutils.exe" ls $env:TEMP | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'winutils.exe does not run' }
Write-Host 'Setup complete. Next: put yellow_tripdata_2026-01.parquet in producer\ and run run_all.bat'
