source scripts/_load_root_env_variables.sh;
echo ENVIRONMENT: $ENVIRONMENT;
echo BRANCH NAME: $GIT_BRANCH;

cd ./packages/api;

docker compose up -d;

cd ../dashboard;

docker compose up -d;

exit 1;