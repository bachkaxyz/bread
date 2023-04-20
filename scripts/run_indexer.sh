source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

if [ "$GIT_BRANCH" == "" ]; then
  echo "no branch set. Please set GIT_BRANCH in root .env";
  exit 0;
fi;

if [ "$ENVIRONMENT" == ""]; then
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
elif [ "$ENVIRONMENT" == "production" ]; then
    echo "production mode";
    cd ./packages/indexer;
    docker compose -f docker-compose.yaml down;
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.yaml build;
    docker compose -f docker-compose.yaml up -d;
    
elif [ "$ENVIRONMENT" == "development" ]; then
    echo development mode;
    cd ./packages/indexer;
    docker compose -f docker-compose.yaml -f docker-compose.local.yaml down;
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.yaml -f docker-compose.local.yaml build;
    docker compose -f docker-compose.yaml -f docker-compose.local.yaml up -d;
elif [ "$ENVIRONMENT" == "testing" ]; then
    echo testing mode;
    echo Using gcloud credentials in $GOOGLE_APPLICATION_CREDENTIALS
    echo If nothing shows up, please set the GOOGLE_APPLICATION_CREDENTIALS
    cd ./packages/indexer;
    docker compose -f docker-compose.tests.yaml down --remove-orphans;
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.tests.yaml build;
    docker compose -f docker-compose.tests.yaml up --abort-on-container-exit;
else
  echo please set ENVIRONMENT to either 'production' or 'development' in root .env;
  exit 0;
fi

exit 1;
