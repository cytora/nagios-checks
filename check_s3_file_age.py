#!/usr/bin/env python

# This Nagios check looks at the age of backup files stored on S3.
# It alerts if files haven't been uploaded within a certain time frame
# and/or alerts if files are too old.
# This script requires authentication credentials to be stored in
# the config file '~/.boto'.
#
#
# .boto file format:
#
#   [Credentials]
#       aws_access_key_id = ABCDEFJKJK39939
#       aws_secret_access_key = 443xkdjksjkldsjfklsdjsdkjsdfkls32xkj2333
#
#

import ConfigParser
import os
import datetime
import dateutil.parser
from dateutil.tz import *
import time
import socket
import boto
import argparse

#Parse command line arguments
parser = argparse.ArgumentParser(description='This script is a Nagios check that \
                                              monitors the age of files that have \
                                              been backed up to an S3 bucket.')

parser.add_argument('--bucketname', dest='bucketname', type=str, required=True,
                    help='Name of S3 bucket')

parser.add_argument('--minfileage', dest='minfileage', type=int, default=0,
                    help='Minimum age of files in an S3 bucket in hours. \
                          Default is 0 hours (disabled).\
                          ')

parser.add_argument('--maxfileage', dest='maxfileage', type=int, default=0,
                    help='Maximum age of files in an S3 bucket in hours. \
                          Default is 0 hours (disabled).')

####
# Add arg option for s3 region?
###

parser.add_argument('--listfiles', action='store_true',
                    help='Enables listing of all files in bucket to stdout. \
                          Use with caution!')

parser.add_argument('--debug', action='store_true',
                    help='Enables debug output.')

args = parser.parse_args()

#Assign variables from command line arguments
bucketname = args.bucketname
minfileage = args.minfileage
maxfileage = args.maxfileage

maxfilecount = 0
minfilecount = 0
totalfilecount = 0

if (args.debug):
    print '########## START DEBUG OUTPUT ############'
    print 'DEBUG: S3 BUCKET NAME: ' + str(bucketname)
    print 'DEBUG: MIN FILE AGE: ' + str(minfileage)
    print 'DEBUG: MAX FILE AGE: ' + str(maxfileage)


if (args.debug):
    print "DEBUG: Connecting to S3"

s3 = boto.connect_s3()

if (args.debug):
    print "DEBUG: S3 Connection: %s" % s3

# Check if bucket exists. Exit with critical if it doesn't
nonexistent = s3.lookup(bucketname)
if nonexistent is None:
    print "CRITICAL: No bucket found with a name of " + str(bucketname)
    exit(2)
else:
    if (args.debug):
        print "DEBUG: Hooray the bucket " + str(bucketname) + " was found!"

bucket = s3.get_bucket(bucketname)
if (args.debug):
    print "Bucket: %s" % bucket

systemhostname = socket.gethostname()


maxagetime = datetime.datetime.now(tzutc()) - datetime.timedelta(hours=maxfileage)
if (args.debug):
    print  'MAX AGE TIME: ' + str(maxagetime)

minagetime = datetime.datetime.now(tzutc()) - datetime.timedelta(hours=minfileage)
if (args.debug):
    print  'MIN AGE TIME: ' + str(minagetime)


for key in bucket.list():
    if (args.listfiles):
        print '|' + str(key.storage_class) + '|' + str(key.name) + '|' \
              + str(dateutil.parser.parse(key.last_modified).replace(tzinfo=tzutc()))
    if dateutil.parser.parse(key.last_modified) < maxagetime:
        if (args.listfiles):
            print 'Found file older than maxfileage of ' + str(maxfileage) + ' hours'
        maxfilecount += 1
    #print key.__dict__
    if dateutil.parser.parse(key.last_modified) > minagetime:
        if (args.listfiles):
            print 'Found file newer than minfileage of ' + str(minfileage) + ' hours'
        minfilecount += 1
    totalfilecount += 1

msg = ' |'
if minfileage > 0:
    msg = msg + ' MIN:' + str(minfileage) + 'hrs'
if maxfileage > 0:
    msg = msg + ' MAX:' + str(maxfileage) + 'hrs'

if maxfileage > 0:
      msg = msg + ' | Files exceeding MAX time: ' + str(maxfilecount)

if minfileage > 0:
      msg = msg + ' | Files meeting MIN time: ' + str(minfilecount)

msg = msg + ' | Total file count: ' + str(totalfilecount)

if minfileage == 0 and maxfileage == 0:
    statusline = 'WARNING: No max or min specified. Please specify at least one.' + msg
    exitcode = 1
elif maxfilecount == 0 and minfilecount > 0:
    statusline = 'OK: No S3 files exceeding time boundaries.' + msg
    exitcode = 0
elif (maxfilecount > 0 or minfilecount == 0) and maxfileage > 0 and minfileage > 0:
    statusline = 'CRITICAL: S3 files exceed time boundaries.' + msg
    exitcode = 2
elif maxfilecount > 0 and maxfileage > 0 and minfileage == 0:
    statusline = 'CRITICAL: S3 files exceed MAX time boundaries.' + msg
    exitcode = 2
elif minfilecount == 0 and maxfileage == 0 and minfileage > 0:
    statusline = 'CRITICAL: S3 files do not meet MIN time requirement.' + msg
    exitcode = 3
elif minfilecount > 0 and maxfileage == 0 and minfileage > 0:
    statusline = 'OK: S3 files meet MIN time boundaries.' + msg
    exitcode = 0
elif maxfilecount == 0 and maxfileage > 0 and minfileage == 0:
    statusline = 'OK: S3 files meet MAX time boundaries.' + msg
    exitcode = 0
else:
    statusline = 'UNKNOWN: ' + msg
    exitcode = 3

print statusline
exit(exitcode)