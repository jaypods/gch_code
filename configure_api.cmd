@echo off
cd /d "%~dp0"
py -3.12 -X utf8 -B configure_api.py
if errorlevel 1 pause
