
source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

if [ $ENVIRONMENT == ""]; then
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
elif [ $ENVIRONMENT == "production" ]; then
    echo "production mode not setup yet";
    exit 0;
elif [ $ENVIRONMENT == "development" ]; then
    echo "development mode";
    cd ./packages/jobs;
    docker compose -f docker-compose.local.yaml down;
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.local.yaml build;
    docker compose -f docker-compose.local.yaml up -d;
else
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
fi

exit 1;
