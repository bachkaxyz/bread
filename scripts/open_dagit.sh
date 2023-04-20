#!/bin/bash
source scripts/_load_root_env_variables.sh;

VM_INT_IP=$(gcloud compute  ssh --command='curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip' --zone "$GCP_COMPUTE_ZONE" "$GCP_COMPUTE_INSTANCE" --project=$GCP_PROJECT_ID)


gcloud compute  ssh --ssh-flag="-L $DAGIT_PORT:$VM_INT_IP:$GCP_INSTANCE_DAGIT_PORT" --zone "$GCP_COMPUTE_ZONE" "$GCP_COMPUTE_INSTANCE" --project=$GCP_PROJECT_ID