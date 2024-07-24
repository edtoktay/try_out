#!/bin/bash
echo 'Stop Docker Containers' &&
docker stop $(docker ps -aq) &&
echo 'Remove Docker Containers' &&
docker rm $(docker ps -aq) &&
echo 'Remove Docker Networks' &&
docker network prune -f &&
echo 'Remove Docker Images' &&
docker rmi -f $(docker images --filter dangling=true -qa) &&
echo 'Remove Docker Volumes' &&
docker volume rm $(docker volume ls --filter dangling=true -q) &&
echo 'Remove Docker Images' &&
docker rmi -f $(docker images -qa)
