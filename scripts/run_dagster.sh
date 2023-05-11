
source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

cd ./packages/dags;
if [ $ENVIRONMENT == ""]; then
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
elif [ $ENVIRONMENT == "production" ]; then
    echo "production mode";
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.local.yaml build;
    docker compose -f docker-compose.prod.yaml -p ${COMPOSE_PREFIX}-dagster up -d;
    exit 0;
elif [ $ENVIRONMENT == "development" ]; then
    echo "development mode";
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.local.yaml build;
    docker compose -f docker-compose.local.yaml up -d;
else
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
fi

exit 1;
