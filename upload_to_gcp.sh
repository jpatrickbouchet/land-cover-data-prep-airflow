#!/bin/bash

gcloud beta composer environments storage dags import \
    --environment landcover-data-prep \
    --location us-central1 \
    --source util/

gcloud beta composer environments storage dags import \
    --environment landcover-data-prep \
    --location us-central1 \
    --source dataflow/

gcloud beta composer environments storage dags import \
    --environment landcover-data-prep \
    --location us-central1 \
    --source dag.py
