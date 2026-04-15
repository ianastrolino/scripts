@echo off
title Frente de Caixa — Vistorias

echo Iniciando servidores...
echo.

:: Moema na porta 8081
start "Moema" wsl -e bash -c "cd /home/astro/projeto/scripts && python3 tiny_import.py --serve-ui --port 8081 --env-file .env.moema >> logs_moema/servidor.log 2>&1"

:: Barueri na porta 8082
start "Barueri" wsl -e bash -c "cd /home/astro/projeto/scripts && python3 tiny_import.py --serve-ui --port 8082 --env-file .env.barueri >> logs_barueri/servidor.log 2>&1"

:: Aguarda 3 segundos e abre os dois no navegador
timeout /t 3 /nobreak > nul

start "" "http://localhost:8081"
start "" "http://localhost:8082"

echo Servidores iniciados.
echo   Moema:   http://localhost:8081
echo   Barueri: http://localhost:8082
echo.
echo Feche esta janela para encerrar os servidores.
pause
