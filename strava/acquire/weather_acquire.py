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
from pymongo import MongoClient
import datetime as dt


# MongoDB Client & DB
client = MongoClient('mongodb://localhost:27017/')
db = client['noaa_weather']


def acquire_metar_records(url,filename,id_list=None):
    hourly_coll = db['hourly_records']

    outFilePath = os.getcwd() + "/" + filename
    month = filename[5:-4]
    try:
        urllib.urlretrieve(url + filename,outFilePath)

        z = zipfile.ZipFile(outFilePath)
        for f in z.namelist():
            if f.find('hourly.txt') > -1:
                #get station info
                with z.open(f,'r') as hourlyFile:
                    csv_dict = csv.DictReader(hourlyFile)
                    for row in csv_dict:
                        if id_list:
                            if row['WBAN'] in id_list:
                                hourly_coll.insert(row)
                        else:
                            hourly_coll.insert(row)
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "Unknown Exception: " + e

def acquire_WBAN_definitions(url):
    wban_coll = db['WBAN']
    csv.register_dialect('WBAN_dialect', delimiter='|')

    zip_file = url.split('/')[-1]
    outFilePath = os.getcwd() + "/" + zip_file
    unzip_file = zip_file[:-4]

    #month = filename[5:-4]
    try:
        urllib.urlretrieve(url,outFilePath)

        z = zipfile.ZipFile(outFilePath)
        with z.open(unzip_file,'r') as wban:
            csv_dict = csv.DictReader(wban,dialect='WBAN_dialect')
            for row in csv_dict:
                wban_coll.insert({k:row[k].decode('utf8','ignore') for k in row}) #decode text, I was getting utf-8 errors without this
        z.close()
        os.remove(outFilePath)
    except Exception as e:
        print "Unknown Exception: " + e



#http://cdo.ncdc.noaa.gov/qclcd_ascii/199607.tar.gz <- filename format before 7/2007
if __name__ == '__main__':
    total_start = dt.datetime.now()
    #get WBAN records
    #acquire_WBAN_definitions('http://www.ncdc.noaa.gov/homr/file/wbanmasterlist.psv.zip')
    #print "Finished collecting WBAN station info. \nTotal Runtime: " + str(dt.datetime.now() - total_start)

    #get CA weather stations
    wban_coll = db['WBAN']
    CA_stations = [station['WBAN_ID'] for station in list(wban_coll.find({'STATE_PROVINCE':'CA'},{'WBAN_ID':1,'_id':0}))]
    #print CA_stations

    months = ['201411','201412','201501','201502']
    for month in months:
        local_start = dt.datetime.now()
        #get hourly weather records for the California stations
        acquire_metar_records('http://cdo.ncdc.noaa.gov/qclcd_ascii/','QCLCD'+month+'.zip',CA_stations)
        print "Finished collecting weather data for " + month + ".\nTotal Runtime: " + str(dt.datetime.now() - local_start)
    print "Finished!\nTotal Run Time: " + str(dt.datetime.now() - total_start)
    print db.command("dbstats")
