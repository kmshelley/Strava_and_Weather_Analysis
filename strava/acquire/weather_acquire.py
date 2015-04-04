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
from pymongo import MongoClient
import datetime as dt
from ..util.config import Config

config = Config()

# MongoDB Client & DB
client = MongoClient(config.get("mongo", "uri"))
db = client[config.get("mongo", "db_strava")]

def acquire_metar_records(url,filename,id_list=None):
    hourly_coll = db['hourly_records']

    outFilePath = filename
    month = filename[5:-4]
    try:
        urllib.urlretrieve(url + filename,outFilePath)

        bulk = db.hourly_coll.initialize_ordered_bulk_op()
        z = zipfile.ZipFile(outFilePath)
        for f in z.namelist():
            if f.find('hourly.txt') > -1:
                #get observation info
                with z.open(f,'r') as hourlyFile:
                    csv_dict = csv.DictReader(hourlyFile)
                    for row in csv_dict:
                        if id_list:
                            if row['WBAN'] in id_list:
                                bulk.insert(row)
                        else:
                            bulk.insert(row)
                result = bulk.execute()
                pprint.pprint(result)
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "####ERROR: " + str(e)

def acquire_WBAN_definitions(url):
    wban_coll = db['WBAN']
    csv.register_dialect('WBAN_dialect', delimiter='|')

    zip_file = url.split('/')[-1]
    outFilePath = zip_file
    unzip_file = zip_file[:-4]
    try:
        urllib.urlretrieve(url,outFilePath)

        z = zipfile.ZipFile(outFilePath)
        with z.open(unzip_file,'r') as wban:
            csv_dict = csv.DictReader(wban,dialect='WBAN_dialect')
            for row in csv_dict:
                if not wban_coll.find_one({'WBAN_ID':row['WBAN_ID']}):
                    #if the WBAN station info is not already in the database, add it
                    wban_coll.insert({k:row[k].decode('utf8','ignore') for k in row}) #decode text, I was getting utf-8 errors without this
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "####ERROR: " + str(e)



#http://cdo.ncdc.noaa.gov/qclcd_ascii/199607.tar.gz <- filename format before 7/2007
if __name__ == '__main__':
    total_start = dt.datetime.now()
    #get WBAN records
    acquire_WBAN_definitions('http://www.ncdc.noaa.gov/homr/file/wbanmasterlist.psv.zip')
    print "Finished collecting WBAN station info. \nTotal Runtime: " + str(dt.datetime.now() - total_start)

    #get CA weather stations
    wban_coll = db['WBAN']
    CA_stations = [station['WBAN_ID'] for station in list(wban_coll.find({'STATE_PROVINCE':'CA'},{'WBAN_ID':1,'_id':0}))]
    #print CA_stations

    months = ['201302','201303','201304','201305',
    '201306','201307','201308','201309','201310','201311','201312',
    '201401','201402','20140303','201404','201405','201406','201407',
    '201408','201409','201410','201411','201412','201501','201502','201503']
    for month in months:
        local_start = dt.datetime.now()
        #get hourly weather records for the California stations
        acquire_metar_records('http://cdo.ncdc.noaa.gov/qclcd_ascii/','QCLCD'+month+'.zip',CA_stations)
        print "Finished collecting weather data for " + month + ".\nTotal Runtime: " + str(dt.datetime.now() - local_start)
    print "Finished!\nTotal Run Time: " + str(dt.datetime.now() - total_start)
    print db.command("dbstats")
