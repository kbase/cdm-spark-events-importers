#!/bin/bash

printf "\n*** starting minio bucket & user setup ***\n"

mc alias set minio $MINIO_URL $MINIO_USER $MINIO_PWD

# make raw bucket
if ! mc ls minio/$MINIO_IMPORTER_RAW_BUCKET 2>/dev/null; then
  mc mb minio/$MINIO_IMPORTER_RAW_BUCKET && echo "Bucket $MINIO_IMPORTER_RAW_BUCKET created"
else
  echo "bucket $MINIO_IMPORTER_RAW_BUCKET already exists"
fi

# make sql bucket
if ! mc ls minio/$MINIO_IMPORTER_SQL_BUCKET 2>/dev/null; then
  mc mb minio/$MINIO_IMPORTER_SQL_BUCKET && echo "Bucket $MINIO_IMPORTER_SQL_BUCKET created"
else
  echo "bucket $MINIO_IMPORTER_SQL_BUCKET already exists"
fi

# create policies
mc admin policy create minio test-importers-read-write-policy /s3_policies/test-importers-read-write-policy.json

# make importer user
mc admin user add minio $MINIO_IMPORTER_USER $MINIO_IMPORTER_PWD
mc admin policy attach minio test-importers-read-write-policy --user=$MINIO_IMPORTER_USER
echo 'importer user and policy set'
