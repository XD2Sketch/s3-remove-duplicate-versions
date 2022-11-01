#!/usr/bin/env python3

import boto3
import argparse
import string
import psycopg
from hurry.filesize import size

# set the file extension for which the dups need to be found for
file_extension = ".fig"

# aws access keys
access_key = "X"
secret_key = "X"
region = "us-east-2"

# database information
database_name = "X"
database_user = "X"
database_password = "X"
database_host = "X"
database_port = "5432"
database_table = "file_backup_history"
database_version_column = "versionId"

parser = argparse.ArgumentParser('Find duplicate objects in an aws s3 bucket')
parser.add_argument('--bucket', dest='myBucket', default='yourBucketName', help='S3 Bucket to search')

cliArgs = parser.parse_args() 

myBucket = cliArgs.myBucket

# each list_objects_v2 request will return up to 1000 objects.
# We will loop for every 1000, make another list_objects_v2 until end of bucket is reached
lastReqLength = 1000

# at the end of each 1000, know the last key so we can get the next 1000 after it
lastKey = ""

existing = {}

s3 = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

print('searching for duplicate objects')
print('')

totalSize = 0

with psycopg.connect("dbname='%s' user=%s password=%s host=%s port=%s" % (database_name, database_user, database_password, database_host, database_port)) as conn:

    with conn.cursor() as cur:

        while lastReqLength == 1000:
            if (lastKey == ""):
                myObjects = s3.list_objects_v2(Bucket=myBucket)
            else:
                myObjects = s3.list_objects_v2(Bucket=myBucket,StartAfter=lastKey)
            lastReqLength = len(myObjects['Contents'])
            for obj in myObjects['Contents']:
                lastKey = obj['Key']
                thisKey = obj['Key']
                thisSize = obj['Size']
                thisEtag = obj['ETag']
                if (file_extension in obj['Key']):
                    existingEtags = {}
                    myVersions = s3.list_object_versions(Bucket=myBucket,Prefix=obj['Key'])
                    for version in myVersions['Versions']:
                        lastKeyVersion = version['Key']
                        thisKeyVersion = version['Key']
                        thisSizeVersion = version['Size']
                        thisEtagVersion = version['ETag']
                        thisVersionId = version['VersionId']
                        if thisSizeVersion > 0:
                            if thisEtagVersion in existingEtags:
                                # Duplicate version found:
                                print('!!Duplicate Version: - Key: %s - Version 1: %s - Version 2: %s - with Size: %s' % (thisKey, existingEtags[thisEtagVersion], thisVersionId, size(thisSizeVersion)))
                                totalSize += thisSizeVersion
                                print('Current size: %s' % (size(totalSize)))
                                # Change database entries
                                cur.execute("""
                                    UPDATE "%s" SET "%s" = '%s' WHERE "%s" = '%s'
                                    """ % (database_table, database_version_column, existingEtags[thisEtagVersion], database_version_column, thisVersionId))
                                conn.commit()
                                # Delete version on AWS S3
                                # TODO: Apparently we also need to get all delete markers and delete them...
                                s3.delete_object(Bucket=myBucket,Key=obj['Key'],VersionId=thisVersionId)
                            else:
                                existingEtags[thisEtagVersion] = thisVersionId
                    if "deleteMarkers" in myVersions:
                        for deleteMarker in myVersions['DeleteMarkers']:
                            print('deleteMarker' % (deleteMarker))

        print('Total Size %s: ' % (size(totalSize)))
        print('... The End.')
