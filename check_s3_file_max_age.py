#!/usr/bin/env python

##########################################################
#
# Written by Matthew McMillan
# matthew.mcmillan@gmail.com
# @matthewmcmillan
# https://matthewcmcmillan.blogspot.com
# https://github.com/matt448/nagios-checks
#
#
# This Nagios check looks at the age of files stored in an S3 bucket.
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
#
# -- Nagios error codes --
#    0 = OK/green
#    1 = WARNING/yellow
#    2 = CRITICAL/red
#    3 = UNKNOWN/purple
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
import re

parser = argparse.ArgumentParser(description='This script is a Nagios check that \
                                              monitors the age of files that have \
                                              been backed up to an S3 bucket.')

parser.add_argument('--bucketname', dest='bucketname', type=str, required=True,
                    help='Name of S3 bucket')

parser.add_argument('--maxfileage', dest='maxfileage', type=int, default=1,
                    help='Maximum age for files in an S3 bucket in hours. \
                          Default is 1 hour.')

parser.add_argument('--minfilecount', dest='minfilecount', type=int, default=1,
                    help='Minimum number of files below with maximum age below limit. \
                          Default is 1.')

parser.add_argument('--bucketfolder', dest='bucketfolder', type=str, default='',
                    help='Folder to check inside bucket (optional).')

parser.add_argument('--debug', action='store_true',
                    help='Enables debug output.')

args = parser.parse_args()

#Assign variables from command line arguments
bucketname = args.bucketname
minfilecount = args.minfilecount
maxfileage = args.maxfileage
bucketfolder  = args.bucketfolder
bucketfolder_regex = '^' + bucketfolder

maxfiles = 0

if (args.debug):
    print '########## START DEBUG OUTPUT ############'
    print 'DEBUG: S3 BUCKET NAME: ' + str(bucketname)
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

#Figure out time delta between current time and max/min file age
maxagetime = datetime.datetime.now(tzutc()) - datetime.timedelta(hours=maxfileage)
if (args.debug):
    print 'MAX AGE TIME: ' + str(maxagetime)

#Loop through keys (files) in the S3 bucket and
#check each one for min and max file age.
for key in bucket.list():
    if re.match(bucketfolder_regex,str(key.name)):
        if dateutil.parser.parse(key.last_modified) >= maxagetime:
            maxfiles += 1
            if maxfiles >= minfilecount:
                break

#Begin formatting status message for Nagios output
#This is conditionally formatted based on requested min/max options.
msg = ' - MAX:' + str(maxfileage) + ' hours'
msg = ' - MIN:' + str(minfilecount) + ' files'
msg = msg + ' - At least ' + str(maxfiles) + ' file' + ("" if maxfiles == 1 else "s") + ' in MAX time'

#I think there probably is a better way of doing this but what I have here works.
#
# Decide exit code for Nagios based on maxfilecount and minfilecount results.
#
# maxfilecount should equal zero for green/OK
# minfilecount should be greater than zero for green/OK
#
if maxfiles < minfilecount:
    statusline = 'CRITICAL: Not enough recent S3 files.' + msg
    exitcode = 2
elif maxfiles >= minfilecount:
    statusline = 'OK: Recent S3 files found.' + msg
    exitcode = 0
else:
    statusline = 'UNKNOWN: ' + msg
    exitcode = 3

print statusline
exit(exitcode)
