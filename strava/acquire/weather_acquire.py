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
from pymongo import MongoClient, GEO2D
import datetime as dt
import sys
from util.config import Config
import contextlib

config = Config()

# MongoDB Client & DB
client = MongoClient(config.get("mongo", "uri"))
db = client[config.get("mongo", "db_strava")]


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
        bulk = db.hourly_records.initialize_ordered_bulk_op()
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
                            if len(list(db.hourly_records.find({'WBAN':wban,'Date':date,'Time':time}))) == 0:
                                _id = db.WBAN.find_one({'WBAN_ID':wban})['_id'] #get the mongo id from WBAN collection
                                bulk.insert(row)
                                bulk.update({'WBAN':wban,'Date':date,'Time':time},{'$set':{'wban_rec_id':_id}}) #add the WBAN coll id for indexing
                                bulk_count+=1
                        if bulk_count == 1000:
                            #perform up to 1000 bulk inserts at a time
                            result = bulk.execute()
                            pprint.pprint(result)
                            bulk_count=0#reset the bulk op count
                            bulk = None
                            bulk = db.WBAN.initialize_ordered_bulk_op()#reset the bulk op
                    result = bulk.execute() #perform a final bulk operation
                    pprint.pprint(result)
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
        bulk = db.WBAN.initialize_ordered_bulk_op()
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
                    decode_row['type'] = 'Point'#geojson coordinate record for indexing;
                    decode_row['coordinates'] = clean_lat_long(row["LOCATION"])#format original text string into lng/lat list
                    bulk.insert(decode_row)
                    bulk_count+=1
                if bulk_count == 1000:
                    #perform up to 1000 inserts at a time
                    result=bulk.execute()
                    pprint.pprint(result)
                    bulk_count=0
                    bulk = None
                    bulk = db.WBAN.initialize_ordered_bulk_op()#reset the bulk op
            #perform a final bulk insert
            result = bulk.execute()
            pprint.pprint(result)
        z.close()
        os.remove(outFilePath)


def convert_lat_lon_strings(string):
    #cleans a latitude or longitude text string into decimal degrees
    from string import punctuation
    for symbol in punctuation.replace('-','').replace('.',''):
        string = string.replace(symbol,' ') #replace punctuation (other than - and .) with space
    coord_list = string.split()
    if coord_list[-1] == 'N' or coord_list[-1] == 'S' or coord_list[-1] == 'E' or coord_list[-1] == 'W':
        if coord_list[-1] == "S" or coord_list[-1] == "W":
            #if the coordinate is in the southern or western hemisphere, the lat/lon is negative.
            if coord_list[0].find('-') == -1: coord_list[0] = '-' + coord_list[0]
        coord_list.pop()#remove the hemisphere indicator
    coordinate = 0
    denominator = 1
    for i in range(len(coord_list)):
        #DMS to decimal formula: deg = D + M/60 + S/3600
        coordinate+=float(coord_list[i])/denominator
        denominator*=60
    if abs(coordinate) > 180:
        return 0
    return coordinate

def clean_lat_long(orig_text):
    #cleans a given a WBAN lat/lon entry, returns a [long, lat] list pair
    try:
        text = str(orig_text)
        for char in text:
            if char.isalpha():
                #if there is an alpha character
                if char not in  ['N','S','E','W']:
                    text = text.replace(char,'') #remove any letters other than NSEW
        #add space between coordinate and hemisphere symbol
        text = text.replace('N',' N').replace('S',' S').replace('E',' E').replace('W',' W')
        if text.find('/') > -1:
            #if the lat long is delineated by a '/'
            latstr,lonstr = text.split('/')[0],text.split('/')[1] #accounts for additional notations in locaiton field ('/altitude')
        elif text.find(',') > -1:
            #comma-separated
            latstr,lonstr = text.split(',')
        elif text.find('N') > -1:
            #split by the north hemisphere symbol
            latstr,lonstr = text.split('N')
            latstr = latstr + 'N' #add north symbol back in
        elif text.find('S') > -1:
            #split by the south hemisphere symbol
            latstr,lonstr = text.split('S')
            latstr = latstr + 'S' #add south symbol back in
        elif text == '':
            #empty location field
            return [0,0]
        else:
            #otherwise print the string and return none
            print "Cannot parse lat/long: %s" % text
            return [0,0]
        lat,lng = convert_lat_lon_strings(latstr),convert_lat_lon_strings(lonstr)
        return [lng,lat]
    except Exception as e:
        print "#####Error parsing lat/long: %s" % orig_text
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
        db.WBAN.ensure_index([('coordinates', GEO2D)])


        #get CA weather stations
        CA_stations = [station['WBAN_ID'] for station in list(db.WBAN.find({'STATE_PROVINCE':'CA'},{'WBAN_ID':1,'_id':0}))]

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

    except Exception as e:
        print "#####ERROR: %s" % e

def get_weather():
    #for running code        
    try:
        collect_and_store_weather_data()
        #clean up any existing data files
        clean_up_files()
    except KeyboardInterrupt,SystemExit:
        print "Interrupted, closing..."
        #clean up existing data files before quitting
        clean_up_files()
    except Exception as e:
        print "#####ERROR: %s" % e
