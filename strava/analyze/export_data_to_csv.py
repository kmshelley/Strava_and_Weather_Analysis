#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Katherine
#
# Created:     01/04/2015
# Copyright:   (c) Katherine 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from pymongo import MongoClient, GEOSPHERE
import datetime as dt
import pprint
import simplekml
import math
import csv
import ast
import os
import sys
import pymongo
from util.config import Config
from util import log
import subprocess
from util import lat_lng

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_data_collection = db[cfg.get("mongo", "coll_zip")]
wban_collection = db[cfg.get("mongo", "coll_wban")]
weather_collection = db[cfg.get("mongo", "coll_weather")]

#data fields to export
segment_fields = os.path.join(os.getcwd(),'strava_segment_fields.txt')
leaderboard_fields = os.path.join(os.getcwd(),'strava_leaderboard_fields.txt')
weather_fields = os.path.join(os.getcwd(),'hourly_record_fields.txt')
    
def decode_dict(D):
    decode = {}
    for key in D:
        try:
            decode[key] = D[key].decode('utf-8','ignore')
        except:
            decode[key] = D[key]
    return decode
    
def join_segment_and_weather(segment_id):
    leader_out = os.path.join(os.getcwd(),'leaderboard.csv')
    weather_out = os.path.join(os.getcwd(),'weather.csv')
    MongoBin = 'C:\\MongoDB\\bin'
    MongoHost = cfg.get("mongo", "uri")[10:-1]
    
    with open(leaderboard_fields,'r') as f:
        leaderboard_header = [line.rstrip() for line in f.readlines()]
    with open(weather_fields,'r') as f:
        weather_header = [line.rstrip() for line in f.readlines()]
     
    wban = leaderboard_collection.find_one({'segment_id':segment_id})['WBAN']
    
##    exportCmd1 = '%s\\mongoexport --host %s --db %s --collection %s --csv --fieldFile %s --query "{segment_id:%s}" --out %s'\
##                % (MongoBin,MongoHost,cfg.get("mongo", "db_strava"),cfg.get("mongo", "coll_leaderboards"),leaderboard_fields,segment['id'],leader_out)
##    exportCmd2 = '%s\\mongoexport --host %s --db %s --collection %s --csv --fieldFile %s --query "{WBAN:\'%s\'}" --out %s'\
##                % (MongoBin,MongoHost,cfg.get("mongo", "db_strava"),cfg.get("mongo", "coll_weather"),weather_fields,wban,weather_out)
##
##    print exportCmd1
##    print exportCmd2
##    
##    subprocess.Popen(exportCmd1)
##    subprocess.Popen(exportCmd2)
    

    
                
    #leaderboard_header = leaderboard_collection.find_one().keys()
    with open('leaderboard.csv','w') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=leaderboard_header,extrasaction='ignore')
        for doc in leaderboard_collection.find({'segment_id':segment_id}):
            try:
                writer.writerow(decode_dict(doc))
            except Exception as e:
                print "###ERROR: %s" % e
                
    with open('weather.csv','w') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=weather_header,extrasaction='ignore')
        for doc in weather_collection.find({'WBAN':wban}):
            try:
                writer.writerow(decode_dict(doc))
            except Exception as e:
                print "###ERROR: %s" % e

    consoleCmds = 'cat leaderboard.csv weather.csv | python mrjob_join.py  > output.txt'
    subprocess.Popen(consoleCmds)
    with open('output.txt','r') as joined:
        with open('joined.csv','w') as csvfile:
            csvfile.writelines([ast.literal_eval(line.split('\t')[1]) for line in joined.readlines()])
            
    

def segment_and_weather_to_csv():

    try:
        #get all segment data in a CSV file
        segment_fields = os.path.join(os.getcwd(),'strava_segment_fields.txt')
        leaderboard_fields = os.path.join(os.getcwd(),'strava_leaderboard_fields.txt')
        weather_fields = os.path.join(os.getcwd(),'hourly_record_fields.txt')
        
        with open(leaderboard_fields,'r') as f:
            leaderboard_header = [line.rstrip() for line in f.readlines()]
        with open(weather_fields,'r') as f:
            weather_header = [line.rstrip() for line in f.readlines()]
        with open(segment_fields,'r') as f:
            segment_header = [line.rstrip() for line in f.readlines()]
            
    ##    with open('segments.csv','w') as csvfile:
    ##        writer = csv.DictWriter(csvfile,fieldnames=segment_header,extrasaction='ignore')
    ##        for doc in segments_collection.find():
    ##            try:
    ##                writer.writerow(decode_dict(doc))
    ##            except Exception as e:
    ##                print "###ERROR: %s" % e
                    
        #find the most popular segment
        segment = list(segments_collection.find().sort('athlete_count',direction=pymongo.DESCENDING).limit(1))[0]
        lat1,lng1,lat2,lng2 = segment['start_latitude'],segment['start_longitude'],segment['end_latitude'],segment['end_longitude']
        direction = lat_lng.bearing_from_two_lat_lons(lat1,lng1,lat2,lng2)#direction of segment
        
        #call def to export data and join with MapReduce
        join_segment_and_weather(segment['id'])
        
        #convert joined MR output to csv
        with open('output.txt','r') as joined:
            with open('joined.csv','w') as csvfile:
                csvfile.write(','.join(weather_header)+','+','.join(leaderboard_header)+',Segment Direction,\n')
                csvfile.writelines([ast.literal_eval(line.split('\t')[1])+','+str(direction)+',\n' for line in joined.readlines()])
    except Exception as e:
        print "#####ERROR: %s" % e
        
        
    
if __name__ == '__main__':
    start = dt.datetime.now()
    segment_and_weather_to_csv()
    print "Runtime: " + str(dt.datetime.now() - start)
