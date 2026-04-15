@echo off
title Frente de Caixa — Moema

echo Iniciando Frente de Caixa - Unidade Moema...

start "Moema" wsl -e bash -c "cd /home/astro/projeto/scripts && python3 tiny_import.py --serve-ui --port 8081 --env-file .env.moema >> logs_moema/servidor.log 2>&1"

timeout /t 3 /nobreak > nul
start "" "http://localhost:8081"

echo Servidor Moema iniciado em http://localhost:8081
pause
