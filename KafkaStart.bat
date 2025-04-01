rem Det här skriptet ser till att Zookeeper och Kafka startas korrekt, även om containrarna inte redan finns. 
rem Skriptet startar containrarna om de inte är igång och gör dem klara för användning.
@echo off
echo Starting Kafka...

:: Kontrollera om Kafka- och Zookeeper-containrarna finns
docker ps -a --filter "name=zookeeper" --filter "name=kafka" >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Containers already exist, checking if they are running...
) ELSE (
    echo Containers not found, creating containers...
    docker-compose -f docker-compose.yml up -d --build
)

:: Kontrollera om Zookeeper är igång
docker inspect --format '{{.State.Running}}' zookeeper >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Zookeeper is running.
) ELSE (
    echo Starting Zookeeper...
    docker-compose up -d zookeeper
)

:: Kontrollera om Kafka är igång
docker inspect --format '{{.State.Running}}' kafka >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Kafka is already running.
) ELSE (
    echo Starting Kafka...
    docker-compose up -d kafka 
)

echo docker Status...
docker ps -a
pause

