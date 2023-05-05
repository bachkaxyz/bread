source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;
echo GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS;


if [ "$GIT_BRANCH" == "" ]; then
  echo "no branch set. Please set GIT_BRANCH in root .env";
  exit 0;
fi;

if [ "$GOOGLE_APPLICATION_CREDENTIALS" == "" ]; then
  echo "no GOOGLE_APPLICATION_CREDENTIALS set. Please set GOOGLE_APPLICATION_CREDENTIALS in root .env";
  exit 0;
fi;

cd ./packages/indexer;
rm creds.json;
rm indexer.log; 

if [ "$ENVIRONMENT" == ""]; then
  echo "please set ENVIRONMENT to either 'production' or 'development' in root .env";
  exit 0;
elif [ "$ENVIRONMENT" == "production" ]; then
    echo "production mode";
    docker compose -f docker-compose.yaml -p ${COMPOSE_PREFIX} down;
    
elif [ "$ENVIRONMENT" == "development" ]; then
    echo development mode;
    docker compose -f docker-compose.yaml -f docker-compose.local.yaml down;
elif [ "$ENVIRONMENT" == "testing" ]; then
    echo testing mode;
    docker compose -f docker-compose.tests.yaml down --remove-orphans;
else
  echo please set ENVIRONMENT to either 'production', 'development' or 'testing' in root .env;
  exit 0;
fi

exit 1;