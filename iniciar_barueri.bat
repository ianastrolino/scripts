@echo off
title Frente de Caixa — Barueri

echo Iniciando Frente de Caixa - Unidade Barueri...

start "Barueri" wsl -e bash -c "cd /home/astro/projeto/scripts && python3 tiny_import.py --serve-ui --port 8082 --env-file .env.barueri >> logs_barueri/servidor.log 2>&1"

timeout /t 3 /nobreak > nul
start "" "http://localhost:8082"

echo Servidor Barueri iniciado em http://localhost:8082
pause
