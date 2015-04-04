#-------------------------------------------------------------------------------
# Downloads ESRI shape files defining zip code boundaries
# loads zip code data into MongoDB collection 'zip_data_coll'
# Data stored includes: State, county, zip code, bounding box points
#
# ZCTA boundaries downloaded from: http://www2.census.gov/geo/tiger/GENZ2013/cb_2013_us_zcta510_500k.zip
# Zip code CSV database downloaded from USPS: http://www.unitedstateszipcodes.org/zip-code-database/
#
# Author:      Katherine Shelley
#
# Created:     3/28/2015
#
#-------------------------------------------------------------------------------
import shapefile
import urllib
import zipfile
import os
from pymongo import MongoClient
import config
import datetime as dt
import csv
import pprint
from ..util import log
from ..util.config import Config

# MongoDB Client & DB for storing zip code data
# MongoDB Client & DB
config = Config()
client = MongoClient(config.get("mongo", "uri"))
db = client[config.get("mongo", "db_strava")]
zip_data_coll = db[config.get("mongo", "coll_zip")]


def import_zip_code_database(csvFile):
    #loads zip code database information from downloaded csv file
    print"Inserting/updating zip code database fron CSV file...\n"
    bulk = zip_data_coll.initialize_ordered_bulk_op()
    csvFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), csvFile)
    print("CSV File: ", csvFile)
    with open(csvFile,'r') as zipcodes:
        csv_dict = csv.DictReader(zipcodes)
        for row in csv_dict:
            zipcode=str(row['zip'])
            for i in range(len(zipcode)-5): zipcode = '0'+zipcode #add zeros to the zip code until it has length 5
            if not zip_data_coll.find_one({'zip':zipcode}):
                bulk.insert({
                    'zip':zipcode,
                    'state':row['state'],
                    'county':row['county'],
                    'type':row['type'],
                    'mid_lat_lng':str([row['latitude'],row['longitude']]),
                    'bbox':''#leave and empty space for the bounding box (will be added later)
                    })
    try:
        result = bulk.execute()
        pprint.pprint(result)
    except Exception as e:
        print "#### ERROR: " + str(e)
        return





def acquire_zip_code_data(url):
    #for acquiring zip code boundary data
    zip_file = url.split('/')[-1]
    outFilePath = zip_file
    unzip_file = zip_file.split('.')[0]
    shp_file = unzip_file + '.shp'
    dbf_file = unzip_file + '.dbf'
    shx_file = unzip_file + '.shx'

    #month = filename[5:-4]
    try:
        print "Downloading Census Bureau shape files...\n"
        urllib.urlretrieve(url,outFilePath)

        z = zipfile.ZipFile(outFilePath)
        print "Extracting files...\n"
        z.extract(shp_file)
        z.extract(dbf_file)
        z.extract(shx_file)
        z.close()

        print "Reading shape files...\n"
        sf = shapefile.Reader(unzip_file)
        for sr in sf.shapeRecords():
            #iterate through the shp and dbf data, adding documents for each zip code
            #to the zip_data_coll in MongoDB
            #get the zip code from the dbf record
            zip_code = sr.record[0]
            if len(list(zip_data_coll.find({'zip':zip_code}))) <> 0:
                #if the zip code is already in the collection, check to see if the
                #bounding box has been defined, update if necessary
                if zip_data_coll.find_one({'zip':zip_code})['bbox'] == '':
                    zip_data_coll.update({'zip': zip_code},{'$set': {'bbox':str(sr.shape.bbox)}})
            else:
                print "Cannot find zip code: " + zip_code

        sf = None
        #remove the shape files and zip file
        os.remove(shp_file)
        os.remove(shx_file)
        os.remove(dbf_file)
        os.remove(zip_file)

        print "Done inserting zip code data.\n"

    except Exception as e:
        print "#### ERROR: " + str(e)
        pass


if __name__ == '__main__':
    start = dt.datetime.now()
    import_zip_code_database('zip_code_database.csv')
    acquire_zip_code_data('http://www2.census.gov/geo/tiger/GENZ2013/cb_2013_us_zcta510_500k.zip')
    print "Total runtime: " + str(dt.datetime.now() - start)

