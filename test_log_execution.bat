@echo off
REM Performance test for --use-log mode with optimizations
REM Test range: 9,000,000 - 9,100,000 (100,000 blocks)
REM Step size: 3 (optimized for Windows)

echo === Performance Test for --use-log Mode ===
echo Block range: 9,000,000 - 9,100,000 (100,000 blocks)
echo Step size: 3 blocks per batch
echo Date: %date% %time%
echo.

REM Test parameters
set START_BLOCK=9000000
set END_BLOCK=9100000
set STEP=3
set LOG_DIR=test_bench_logs

REM Check if log directory exists
if not exist "%LOG_DIR%" (
    echo Error: Log directory %LOG_DIR% not found
    echo Please ensure logs are generated first with --log-block on
    exit /b 1
)

REM Check if mmap log exists
set MMAP_FLAG=
if exist "%LOG_DIR%\state_logs_mmap.bin" (
    echo Found mmap log file: state_logs_mmap.bin
    set MMAP_FLAG=--mmap-log
) else (
    echo Using file-based log mode (no mmap)
)

echo.
echo Starting test...
echo Command: target\release\pevm.exe evm --begin %START_BLOCK% --end %END_BLOCK% --step %STEP% --use-log on --log-dir %LOG_DIR% %MMAP_FLAG%
echo.

REM Run the test with timing
powershell -Command "Measure-Command { .\target\release\pevm.exe evm --begin %START_BLOCK% --end %END_BLOCK% --step %STEP% --use-log on --log-dir %LOG_DIR% %MMAP_FLAG% | Out-Host } | Select-Object TotalSeconds"

echo.
echo === Test Completed ===
pause
