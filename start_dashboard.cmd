@echo off
cd /d "%~dp0"
py -3.12 -X utf8 -B launch_simple.py --runtime-root "%~dp0runtime" --port 58446
if errorlevel 1 pause
