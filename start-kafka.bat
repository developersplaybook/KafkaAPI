@echo off
echo ========================================
echo Starting Kafka Development Environment
echo ========================================
echo.

echo Cleaning up old data...
rmdir /s /q data 2>nul
rmdir /s /q logs 2>nul

echo Stopping any existing containers...
docker-compose down -v 2>nul

echo Starting services...
docker-compose up -d

echo.
echo Waiting for Kafka to be ready...
timeout /t 10 /nobreak >nul

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >nul 2>&1
if %errorlevel% neq 0 (
    echo Kafka is not ready yet, waiting...
    timeout /t 10 /nobreak >nul
)

echo.
echo ========================================
echo Kafka is running!
echo ========================================
echo Kafka Broker: localhost:9092
echo Kafka UI: http://localhost:8080
echo ========================================
echo.
echo To stop: docker-compose down
echo To view logs: docker-compose logs -f
echo.
pause