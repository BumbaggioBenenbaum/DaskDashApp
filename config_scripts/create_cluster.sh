#!/bin/bash

aws emr create-cluster \
--name DASK \
--use-default-roles \
--release-label emr-5.29.0 \
--instance-count 6 \
--instance-type m4.xlarge \
--applications Name=Hadoop \
--log-uri s3://dasklogs \
--bootstrap-actions Path=s3://daskdata21/assets/bootstrap.sh,Args="[--conda-packages,bokeh,fastparquet,python-snappy,snappy]" \
--ec2-attributes KeyName=bdw,EmrManagedMasterSecurityGroup=sg-0f6fa6022c4a84adb,EmrManagedSlaveSecurityGroup=sg-0f6fa6022c4a84adb


#--bootstrap-actions Path=s3://daskdata21/assets/bootstrap.sh \
