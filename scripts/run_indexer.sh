
source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

if [ $ENVIRONMENT == "production" ]; then
    cd ./packages/indexer;
    envsubst < ./docker-compose.yaml > ./docker-compose.yaml;
    docker compose -f docker-compose.yaml down;
    docker compose -f docker-compose.yaml up -d;
    git checkout -- ./docker-compose.yaml;
elif [ $ENVIRONMENT == "development" ]; then
    echo "development mode";
    cd ./packages/indexer;
    sudo docker compose -f docker-compose.yaml -f docker-compose.local.yaml down;
    sudo docker compose -f docker-compose.yaml -f docker-compose.local.yaml up -d;
else
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
fi

exit 1;
