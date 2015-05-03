#-------------------------------------------------------------------------------
# Name:        data_backup_restore.py
# Purpose:     Exports data from MongoDB into S3 buckets
#
# Author:      Katherine Shelley
#
# Created:     4/25/2015
#-------------------------------------------------------------------------------

import os
import shutil
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from filechunkio import FileChunkIO
from pymongo import MongoClient
import datetime as dt
import sys
import math
import zipfile
import subprocess
from util.config import Config

config = Config()

# MongoDB Client & DB
client = MongoClient(config.get("mongo", "uri"))
host = config.get("mongo","host")
port = config.get("mongo","port")
db = client[config.get("mongo", "db_strava")]
mongo_bin = config.get("mongo","mongo_bin")

# AWS connection
conn = S3Connection(config.get("aws","access_key_id"), config.get("aws","secret_access_key"))
base_bucket = config.get("aws","bucket_name")
try:
    bucket = conn.create_bucket(base_bucket)
except Exception as e:
    print "#####ERROR: %s" % e
    #print "Bucket %s already exists!" % base_bucket
    #bucket = conn.get_bucket(base_bucket)

def backup_mongo_to_s3(db_name,coll):
    #set up and call mongodump.exe
    out_path = os.path.join(os.getcwd(),coll)#the data dump path
    dump_command = mongo_bin + 'mongodump --host ' + host + ' --port \
                ' + port + ' --db ' + db_name + ' --collection ' + coll + '\
                --out ' + out_path #terminal mongodump command
    print "Running mongodump..."
    subprocess.call(dump_command)
    #zip the folder
    z = zipfile.ZipFile(out_path + '.zip','w',zipfile.ZIP_DEFLATED,allowZip64=True)
    for dirname, subdirs, files in os.walk(out_path):
        for filename in files:
            z.write(os.path.join(dirname, filename),db_name + '\\' + filename)
    z.close()
    try:
        shutil.rmtree(out_path)
    except Exception as e:
        print "#####ERROR: %s" % e
    #upload the backups to S3 bucket
    print "Backing up data to S3..."
    b = Key(bucket)
    b.key = coll + '.zip'
    b.set_contents_from_filename(os.path.join(os.getcwd(),coll + '.zip'))#set the contents of the key from the local file
    #delete the local compressed file
    try:
        os.remove(os.path.join(os.getcwd(),coll+'.zip'))
    except Exception as e:
        print "#####ERROR: %s" % e

def restore_mongo_from_s3(db_name,coll):
    print "Restoring data from S3..."
    # Restores the MongoDB db_name from files downloaded from the given S3 bucket
    zip_file = os.path.join(os.getcwd(),coll+ '.zip')
    unzip_file = os.path.join(os.getcwd(),coll)
    for key in bucket.list():
        #iterate through the keys, downloading contents
        if key.name == coll + '.zip':
            key.get_contents_to_filename(zip_file)

    z = zipfile.ZipFile(zip_file,allowZip64=True)
    print "Extracting files...\n"
    z.extractall(unzip_file)
    z.close()
    try:
        os.remove(zip_file)
    except Exception as e:
        print "#####ERROR: %s" % e

    #set up and call mongorestore.exe
    restore_command = mongo_bin + 'mongorestore --host ' + host + ' --port \
                ' + port + ' --db ' + db_name + ' ' + unzip_file + '//' + db_name
    print "Running mongorestore..."
    subprocess.call(restore_command)
    try:
        shutil.rmtree(unzip_file)
    except Exception as e:
        print "#####ERROR: %s" % e


def run_full_backup():
    for coll in db.collection_names():
        start = dt.datetime.now()
        backup_mongo_to_s3(db.name,coll)
        print "Done backing up %s. Runtime: %s" % (coll,(dt.datetime.now() - start))

def run_full_restore():
    collections=['WBAN','zip','segments','hourly_records','leaderboards']
    for coll in collections:
            start = dt.datetime.now(db.name,coll)
            restore_mongo_from_s3()
            print "Done backing up %s. Runtime: %s" % (coll,(dt.datetime.now() - start))

