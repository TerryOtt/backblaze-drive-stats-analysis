#!/usr/bin/bash

S3_ENDPOINT_URL="https://s3.us-west-004.backblazeb2.com"
SOURCE_S3_URL="s3://drivestats-iceberg/drivestats"
AWS_ACCESS_KEY_ID="0045f0571db506a0000000017"
AWS_SECRET_ACCESS_KEY="K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE"
AWS_REGION="us-west-004"
AWS_CLI="/snap/bin/aws"

echo "       Access Key: ${AWS_ACCESS_KEY_ID}"
echo "Secret Access Key: ${AWS_SECRET_ACCESS_KEY}"
echo "           Region: ${AWS_REGION}"

# ${AWS_CLI} configure list
${AWS_CLI} s3 sync --endpoint-url=${S3_ENDPOINT_URL} ${SOURCE_S3_URL} /mnt/b2_mirror/drivestats
