rem Det här skriptet stoppar och tar bort Kafka- och Zookeeper-containrarna, samt eventuella resurser som inte längre behövs.
@echo off
echo Stopping Kafka...

:: Stopp och ta bort Kafka och Zookeeper containrar
docker-compose down

:: Bekräfta att containrarna har stoppats och tagits bort
docker ps -a --filter "name=zookeeper" --filter "name=kafka" >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Containers still exist. Stopping them.
    docker stop zookeeper kafka
    docker rm zookeeper kafka
) ELSE (
    echo Containers are already stopped and removed.
)

echo Kafka and Zookeeper have been stopped.
pause
