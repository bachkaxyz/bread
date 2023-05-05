
source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

cd ./packages/dags;
if [ $ENVIRONMENT == ""]; then
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
elif [ $ENVIRONMENT == "production" ]; then
    echo "production mode";
    docker compose -f docker-compose.prod.yaml -p ${COMPOSE_PREFIX} down;
    exit 0;
elif [ $ENVIRONMENT == "development" ]; then
    echo "development mode";
    docker compose -f docker-compose.local.yaml down;
else
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
fi

exit 1;
