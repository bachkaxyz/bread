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
    export GIT_BRANCH=$GIT_BRANCH;
    docker compose -f docker-compose.yaml down;
    docker compose -f docker-compose.yaml up -d;
    
elif [ "$ENVIRONMENT" == "development" ]; then
    echo "development mode";
    cd ./packages/indexer;
    docker compose -f docker-compose.yaml -f docker-compose.local.yaml down;
    DOCKER_BUILDKIT=0 docker compose -f docker-compose.yaml -f docker-compose.local.yaml build;
    docker compose -f docker-compose.yaml -f docker-compose.local.yaml up -d;
else
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
fi

exit 1;
