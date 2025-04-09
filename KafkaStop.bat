:: This script stops and removes the Kafka and Zookeeper containers, as well as any resources that are no longer needed
@echo off
echo Stopping Kafka...

:: Stop and remove Kafka and Zookeeper containers
docker-compose down

:: Confirm that containers are stopped and removed
docker ps -a --filter "name=zookeeper" --filter "name=kafka" >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Containers still exist. Stopping them.
    docker stop zookeeper kafka control-center
    docker rm zookeeper kafka control-center
) ELSE (
    echo Containers are already stopped and removed.
)

echo Kafka and Zookeeper have been stopped.
pause
