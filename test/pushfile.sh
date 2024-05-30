#!/bin/bash
BUCKET="fldoh-as2test1"
PROCESSED_PATH="fldoh-as2test1/inbox/processed"
UUID=`uuidgen | tr A-F a-f`

aws s3 cp helloworld s3://${BUCKET}/${PROCESSED_PATH}/${UUID}
