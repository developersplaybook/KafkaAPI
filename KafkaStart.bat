:: This script ensures that Zookeeper and Kafka start correctly, even if the containers do not already exist.
:: script starts the containers if they are not running and prepares them for use.

@echo off
echo Starting Kafka...

:: Check if Kafka- and Zookeeper-containrarna exists
docker ps -a --filter "name=zookeeper" --filter "name=kafka" >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Containers already exist, checking if they are running...
) ELSE (
    echo Containers not found, creating containers...
    docker-compose -f docker-compose.yml up -d --build
)

:: Check if Zookeeper is running
docker inspect --format '{{.State.Running}}' zookeeper >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Zookeeper is running.
) ELSE (
    echo Starting Zookeeper...
    docker-compose up -d zookeeper
)

:: Check if Kafka is running
docker inspect --format '{{.State.Running}}' kafka >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Kafka is already running.
) ELSE (
    echo Starting Kafka...
    docker-compose up -d kafka 
)

:: Check if control-center is running
docker inspect --format '{{.State.Running}}' control-center >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo control-center is already running.
) ELSE (
    echo Starting control-center...
    docker-compose up -d control-center 
)


sleep 3
echo docker Status...
docker ps -a
pause

