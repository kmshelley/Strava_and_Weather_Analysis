#-------------------------------------------------------------------------------
# Name:        weather_acquire.py
# Purpose:     Downloads weather data from NOAA into MongoDB
#
# Author:      Katherine Shelley
#
# Created:     3/16/2015
#-------------------------------------------------------------------------------

import urllib
import os
import zipfile
import csv
import pprint
from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError
import datetime as dt
import sys
from util.config import Config
from util import lat_lng
import contextlib
import ast

config = Config()

# MongoDB Client & DB
client = MongoClient(config.get("mongo", "uri"))
db = client[config.get("mongo", "db_strava")]

batch_size = ast.literal_eval(config.get("mongo","batch_size"))


def clean_up_files():
    import glob
    try:
        if os.path.isfile(os.path.join(os.getcwd(),'wbanmasterlist.psv.zip')):
            #if the WBAN file exists, delete it
            os.remove(os.path.join(os.getcwd(),'wbanmasterlist.psv.zip'))
        #get a list of existing weather observation files
        weather_files = glob.glob(os.getcwd() + '/QCLCD*')
        for file in weather_files: os.remove(file)
    except:
        "Error deleting weather files!"
        return


def acquire_metar_records(url,filename,id_list=None):
    #hourly_coll = db['hourly_records']

    outFilePath = os.path.join(os.getcwd(),filename)
    month = filename[5:-4]

    urllib.urlretrieve(url + filename,outFilePath)
    if os.path.isfile(outFilePath) and zipfile.is_zipfile(outFilePath):
        #if the url passed to the def exists and is a valid zip file
        #added for Linux (was creating an empty file for non-existent url downloads)
        bulk = db.hourly_records.initialize_unordered_bulk_op()
        bulk_count=0#for keeping track of bulk operations
        z = zipfile.ZipFile(outFilePath)
        for f in z.namelist():
            if f.find('hourly.txt') > -1:
                #get observation info
                with contextlib.closing(z.open(f,'r')) as hourlyFile:
                    csv_dict = csv.DictReader(hourlyFile)
                    for row in csv_dict:
                        wban,date,time = row['WBAN'],row['Date'],row['Time']
                        if row['WBAN'] in id_list or id_list is None:
                            #if the WBAN ID is in the id list of stations to search,
                            #or if we are searching all stations (id_list not specified)
                            _id = wban+'_'+date+'_'+time #custom mongodb id
                            row['_id'] = _id
                            row['search_idx'] = _id[:-2]
                            bulk.insert(row)
                            bulk_count+=1
                        if bulk_count == batch_size:
                            #perform up to 'batch_size' bulk inserts at a time
                            try:
                                #perform a final bulk insert
                                result = bulk.execute()
                                pprint.pprint(result)
                            except BulkWriteError as bwe:
                                 pprint.pprint(bwe.details)
                            except Exception as e:
                                print "#####ERROR: %s" % e
                            bulk_count=0#reset the bulk op count
                            bulk = None
                            bulk = db.hourly_records.initialize_unordered_bulk_op()#reset the bulk op
                    try:
                        #perform a final bulk insert
                        result = bulk.execute()
                        pprint.pprint(result)
                    except BulkWriteError as bwe:
                        pprint.pprint(bwe.details)
                    except Exception as e:
                        print "#####ERROR: %s" % e
                        
        z.close()
        os.remove(outFilePath)

def acquire_WBAN_definitions(url):
    csv.register_dialect('WBAN_dialect', delimiter='|') #WBAN file is PSV

    zip_file = url.split('/')[-1]
    outFilePath = os.path.join(os.getcwd(),zip_file)
    unzip_file = zip_file[:-4]

    urllib.urlretrieve(url,outFilePath)
    if os.path.isfile(outFilePath) and zipfile.is_zipfile(outFilePath):
        #if the url passed to the def exists and is a valid zip file
        #added for Linux (was creating an empty file for non-existent url downloads)
        bulk = db.WBAN.initialize_unordered_bulk_op()
        bulk_count = 0 #for chunking bulk operations
        z = zipfile.ZipFile(outFilePath)
        with contextlib.closing(z.open(unzip_file,'r')) as wban:
            csv_dict = csv.DictReader(wban,dialect='WBAN_dialect')
            for row in csv_dict:
                if not db.WBAN.find_one({'WBAN_ID':row['WBAN_ID']}):
                    #if the WBAN station info is not already in the database, add it
                    decode_row = {}
                    for k in row: decode_row[k] = row[k].decode('utf-8','ignore') #decode text, I was getting utf-8 errors without this
                    #add geojson point based on "LOCATION" field, for indexing
                    decode_row['loc'] = {'type':'Point','coordinates':lat_lng.clean_lat_long(row['LOCATION'])}#format original text string into lng/lat list into geojson format
                    bulk.insert(decode_row)
                    bulk_count+=1
                if bulk_count == batch_size:
                    #perform up to 'batch_size' inserts at a time
                    try:
                        #perform a final bulk insert
                        result = bulk.execute()
                        pprint.pprint(result)
                    except BulkWriteError as bwe:
                        pprint.pprint(bwe.details)
                    except Exception as e:
                        print "#####ERROR: %s" % e
                    bulk_count=0
                    bulk = None
                    bulk = db.WBAN.initialize_unordered_bulk_op()#reset the bulk op
            try:
                #perform a final bulk insert
                result = bulk.execute()
                pprint.pprint(result)
            except BulkWriteError as bwe:
                pprint.pprint(bwe.details)
            except Exception as e:
                print "#####ERROR: %s" % e
        z.close()
        os.remove(outFilePath)



def update_hourly_records_with_new_index():
    bulk = db.hourly_records.initialize_unordered_bulk_op()
    bulk_count = 0 #for chunking bulk operations
    for doc in db.hourly_records.find():
        bulk.find({'_id': doc['_id']}).update({'$set': {'search_idx':doc['_id'][:-2]}})
        bulk_count+=1
        if bulk_count == batch_size:
            #perform up to 'batch_size' inserts at a time
            try:
                #perform a final bulk insert
                result = bulk.execute()
                pprint.pprint(result)
            except BulkWriteError as bwe:
                pprint.pprint(bwe.details)
            except Exception as e:
                print "#####ERROR: %s" % e
            bulk_count=0
            bulk = None
            bulk = db.hourly_records.initialize_unordered_bulk_op()#reset the bulk op
    try:
        #perform a final bulk insert
        result = bulk.execute()
        pprint.pprint(result)
        db.hourly_records.ensure_index([('search_idx', 1)])
    except BulkWriteError as bwe:
        pprint.pprint(bwe.details)
    except Exception as e:
        print "#####ERROR: %s" % e


#http://cdo.ncdc.noaa.gov/qclcd_ascii/199607.tar.gz <- filename format before 7/2007
def collect_and_store_weather_data():
    try:
        total_start = dt.datetime.now()

        #get WBAN records
        acquire_WBAN_definitions('http://www.ncdc.noaa.gov/homr/file/wbanmasterlist.psv.zip')
        print "Finished collecting WBAN station info. \nTotal Runtime: " + str(dt.datetime.now() - total_start)

        #index WBAN station collection by coordinate (GEO2D index)
        print "Indexing coordinates..."
        db.WBAN.ensure_index([('loc', GEOSPHERE )])


        #get CA weather stations
        CA_stations = [station['WBAN_ID'] for station in list(db.WBAN.find({'STATE_PROVINCE':'CA'},{'WBAN_ID':1,'_id':0}))]

        months = range(12,0,-1)
        years = range(2015,2010,-1)
        for year in years:
            for month in months:
                local_start = dt.datetime.now()
                #get hourly weather records for the California stations
                acquire_metar_records('http://cdo.ncdc.noaa.gov/qclcd_ascii/','QCLCD%04d%02d.zip' % (year,month),CA_stations)
                print "Finished collecting weather data for %04d%02d." % (year,month)
                print "Total Runtime: %s " % (dt.datetime.now() - local_start)
        print "Finished!\nTotal Run Time: %s " % (dt.datetime.now() - total_start)
        print db.command("dbstats")

    except Exception as e:
        print "#####ERROR: %s" % e

def get_weather():
    #for running code
    try:
        #collect_and_store_weather_data()
        #clean up any existing data files
        #clean_up_files()
        update_hourly_records_with_new_index()
    except KeyboardInterrupt,SystemExit:
        print "Interrupted, closing..."
        #clean up existing data files before quitting
        clean_up_files()
    except Exception as e:
        print "#####ERROR: %s" % e
