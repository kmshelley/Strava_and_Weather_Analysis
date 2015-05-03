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
import random
import pymongo
from ..util.config import Config
from ..util import log
import subprocess
from ..util import lat_lng
import re

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

#Date and time formats
wban_date_format = cfg.get("weather","date_format")
strava_datetime_format = cfg.get("strava","date_time_format")

#data fields to export
segment_header = cfg.get("mapreduce","segment_fields").split(',')
leaderboard_header = cfg.get("mapreduce","leaderboard_fields").split(',')
weather_header = cfg.get("mapreduce","weather_fields").split(',')

#output files
leader_out='leaderboards.csv'
weather_out='weather.csv'

def decode_dict(D):
    decode = {}
    for key in D:
        try:
            decode[key] = D[key].decode('utf-8','ignore')
        except:
            decode[key] = D[key]
    return decode

def concatenate_files(file1,file2,output):
    with open(output,'w') as out:
        with open(file1,'r') as f:
            for line in f: out.write(line + '\n')
        with open(file2,'r') as f:
            for line in f: out.write(line + '\n')
    return output


def export_segment(query={}):
    #leaderboard_header = leaderboard_collection.find_one().keys()
    with open(leader_out,'w') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=leaderboard_header,extrasaction='ignore')
        for doc in leaderboard_collection.find(query):
            try:
                writer.writerow(decode_dict(doc))
            except Exception as e:
                print "###ERROR: %s" % e
    return leader_out

def export_weather(query={}):
    with open(weather_out,'w') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=weather_header,extrasaction='ignore')
        for doc in weather_collection.find(query):
            try:
                writer.writerow(decode_dict(doc))
            except Exception as e:
                print "###ERROR: %s" % e
    return weather_out


def join_segment_and_weather(segment_query):
    data_out = os.path.join(os.getcwd(),'joined.csv')

    wban_list = list(set([wban['WBAN'] for wban in list(leaderboard_collection.find(segment_query,{'_id':0,'WBAN':1}))])) #list of wban station ids
    wban_query = {'WBAN':{'$in':wban_list}}
    leaders = export_segment(segment_query)
    weather = export_weather(wban_query)

    concatenated = concatenate_files(leaders,weather,'concat.txt')
    consoleCmds = 'python .\\analyze\\mrjob_join.py'
    print consoleCmds

    with open(concatenated,'r') as concat:
        with open('output.txt','w') as output:
            p = subprocess.Popen(consoleCmds, stdin=concat, stdout=output)
    p.wait() #wait for the command to finish

    with open('joined.csv','w') as csv:
        with open('output.txt','r') as output:
            for line in output:
                try:
                    csv.write(ast.literal_eval(line.split('\t')[1]) + '\n')
                except:
                    pass
    os.remove('output.txt')
    os.remove(concatenated)
    os.remove(leaders)
    os.remove(weather)
    return data_out

def random_segments_and_weather_to_csv(n,m):
    #get random sample of m leaderboard entries from n segments and weather data in a CSV file
    data_out = os.path.join(os.getcwd(),'joined.csv')
    try:
        #csv field names
        fieldnames = weather_header+leaderboard_header+['Segement Direction']

        #iterate through 50 random segments, join random sample of leaderboard data with historical weather into CSV
        with open('joined.csv','w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames,extrasaction='ignore')
            writer.writeheader()

            #get 50 random segments from data set
            random_segments = get_random_mongo_data(segments_collection,n)
            for segment in random_segments:
                lat1,lng1,lat2,lng2 = segment['start_latitude'],segment['start_longitude'],segment['end_latitude'],segment['end_longitude']
                direction = lat_lng.bearing_from_two_lat_lons(lat1,lng1,lat2,lng2)#direction of segment

                #get 10 random leaderboard efforts from this segment
                random_leaders = get_random_mongo_data(leaderboard_collection,m,{'segment_id':segment['id']})
                for leader in random_leaders:
                    #pprint.pprint(leader)
                    wban = leader['WBAN']
                    strava_time = dt.datetime.strptime(leader['start_date_local'],strava_datetime_format)
                    date = dt.datetime.strftime(strava_time,wban_date_format)
                    hour = dt.datetime.strftime(strava_time,'%H')

                    weather_id = '%s_%s_%s' % (wban,date,hour)#alternate weather id field (indexed in Mongo)
                    weather_obs = None
                    weather_obs = weather_collection.find_one({'search_idx':weather_id})
                    merged_doc = {}#merged dictionary of leaderboard and weather records
                    merged_doc.update(leader)
                    #merged_doc = leader.copy()
                    if weather_obs: merged_doc.update(weather_obs)
                    merged_doc['Segment Direction'] = direction
                    writer.writerow(merged_doc)
        return data_out
    except Exception as e:
        if merged_doc: pprint.pprint(merged_doc)
        print "#####ERROR: %s" % e

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

def get_random_mongo_data(mongoColl,n,query={}):
    #returns a list of n random records from a Mongo collection
    collection = []
    M = mongoColl.find(query).count() #total size of query results
    for i in range(n):
        collection.append(list(mongoColl.find(query).skip(int(random.random()*M)).limit(1))[0])
    return collection


if __name__ == '__main__':
    start = dt.datetime.now()
    #segment_and_weather_to_csv()
    random_segments_and_weather_to_csv()

    print "Runtime: " + str(dt.datetime.now() - start)
