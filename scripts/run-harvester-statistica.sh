#!/bin/bash

HARVESTER="harvester -vvv --debug"
DATABASE="mongodb+mongodb://database.local/harvester_data"
JOBID="$( date +%s )"


# Download data from dati.trentino.it

$HARVESTER crawl --crawler ckan+http://dati.trentino.it \
    --storage "${DATABASE}"/"$JOBID".ckan_dti


# Download data from statistica

$HARVESTER crawl \
    --crawler pat_statistica \
    --storage "${DATABASE}"/"$JOBID".statistica

$HARVESTER convert \
    --converter pat_statistica_to_ckan \
    --input "${DATABASE}"/"$JOBID".statistica \
    --output "${DATABASE}"/"$JOBID".statistica_clean


# Download data from statistica-subpro

$HARVESTER crawl \
    --crawler pat_statistica_subpro \
    --storage "${DATABASE}"/"$JOBID".statistica_subpro

$HARVESTER convert \
    --converter pat_statistica_subpro_to_ckan \
    --input "${DATABASE}"/"$JOBID".statistica_subpro \
    --output "${DATABASE}"/"$JOBID".statistica_subpro_clean


cat <<EOF

All done
========

Data is stored in the following databases:

${DATABASE}/${JOBID}.ckan_dti
${DATABASE}/${JOBID}.statistica
${DATABASE}/${JOBID}.statistica_clean
${DATABASE}/${JOBID}.statistica_subpro
${DATABASE}/${JOBID}.statistica_subpro_clean
EOF
