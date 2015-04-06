#-------------------------------------------------------------------------------
# Name:        weather_acquire.py
# Purpose:     Downloads weather data from NOAA into MongoDB
#
# Author:      Katherine Shelley
#
# Created:     3/16/2015
#-------------------------------------------------------------------------------
import contextlib
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

        bulk = hourly_coll.initialize_ordered_bulk_op()
        z = zipfile.ZipFile(outFilePath)
        for f in z.namelist():
            if f.find('hourly.txt') > -1:
                #get observation info
                with contextlib.closing(z.open(f,'r')) as hourlyFile:
                    csv_dict = csv.DictReader(hourlyFile)
                    for row in csv_dict:
                        wban,date,time = row['WBAN'],row['Date'],row['Time']
                        #if the WBAN is in the ID list and the hourly record has not yet been inserted
                        if wban in id_list and len(list(hourly_coll.find({'WBAN':wban,'Date':date,'Time':time}))) == 0:
                            bulk.insert(row)
                result = bulk.execute()
                pprint.pprint(result)
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "####ERROR: %s" % e

def acquire_WBAN_definitions(url):
    wban_coll = db['WBAN']
    csv.register_dialect('WBAN_dialect', delimiter='|')

    zip_file = url.split('/')[-1]
    outFilePath = zip_file
    unzip_file = zip_file[:-4]
    try:
        urllib.urlretrieve(url,outFilePath)

        z = zipfile.ZipFile(outFilePath)
        with contextlib.closing(z.open(unzip_file,'r')) as wban:
            csv_dict = csv.DictReader(wban,dialect='WBAN_dialect')
            for row in csv_dict:
                if not wban_coll.find_one({'WBAN_ID':row['WBAN_ID']}):
                    #if the WBAN station info is not already in the database, add it
                    wban_coll.insert(row) 
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "####ERROR: %s" % e

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

   months = range(12,0,-1)
    years = range(2015,2012,-1)
    for year in years:
        for month in months:
            local_start = dt.datetime.now()
            #get hourly weather records for the California stations
            acquire_metar_records('http://cdo.ncdc.noaa.gov/qclcd_ascii/','QCLCD%04d%02d.zip' % (year,month),CA_stations)
            print "Finished collecting weather data for %04d%02d." % (year,month)
            print "Total Runtime: " + str(dt.datetime.now() - local_start)    
    print "Finished!\nTotal Run Time: " + str(dt.datetime.now() - total_start)
    print db.command("dbstats")
