@echo off
echo.
echo ========================================
echo   INICIANDO AIRFLOW - ETL BRASIL.IO
echo ========================================
echo.

REM Verificar se Docker está rodando
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Docker nao esta rodando!
    echo.
    echo Inicie o Docker Desktop e tente novamente.
    pause
    exit /b 1
)

echo [OK] Docker esta rodando
echo.

REM Criar pastas necessárias
if not exist "logs" mkdir logs
if not exist "plugins" mkdir plugins

echo [INFO] Iniciando containers do Airflow...
docker-compose up -d

echo.
echo ========================================
echo   AIRFLOW INICIADO COM SUCESSO!
echo ========================================
echo.
echo Acesse: http://localhost:8080
echo Usuario: admin
echo Senha: admin
echo.
echo Aguarde ~30 segundos para inicializacao completa
echo.
echo Para parar: docker-compose down
echo Para ver logs: docker-compose logs -f
echo.
pause
