#################
# Variables
##################

DOCKER_COMPOSE = docker compose -f ./docker-compose.yaml

##################
# Docker compose
##################

build:
  ${DOCKER_COMPOSE} build
start:
  ${DOCKER_COMPOSE} start
stop:
  ${DOCKER_COMPOSE} stop
up:
  ${DOCKER_COMPOSE} up -d
ps:
  ${DOCKER_COMPOSE} ps
logs:
  ${DOCKER_COMPOSE} logs -f
down:
  ${DOCKER_COMPOSE} down -v --rmi=all --remove-orphans
restart:
  make stop start
rebuild:
  make down build up
