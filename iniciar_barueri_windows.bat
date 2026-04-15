@echo off
title Frente de Caixa — Barueri
cd /d "%~dp0"

echo Iniciando Frente de Caixa - Unidade Barueri...
echo.

start "Barueri" python tiny_import.py --serve-ui --port 8082 --env-file .env.barueri

timeout /t 3 /nobreak > nul
start "" "http://localhost:8082"

echo Servidor iniciado em http://localhost:8082
echo Pode fechar esta janela.
