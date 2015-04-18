#-------------------------------------------------------------------------------
# Name:        Strava debug scratch pad and testing
# Purpose:
#
# Author:      Katherine
#
# Created:     03/04/2015
#-------------------------------------------------------------------------------
import sys, os
from report.google_polyline_encoder import decode
import simplekml
import datetime as dt
import csv
import pprint
import re
import pandas
from pymongo import MongoClient, GEOSPHERE
from util import log
from util.config import Config
from bson.code import Code
from bson.son import SON

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_data_collection = db[cfg.get("mongo", "coll_zip")]
weather_collection = db[cfg.get("mongo","coll_weather")]
wban_collection = db[cfg.get("mongo","coll_wban")]

wban_date_format = cfg.get("weather","date_format")
wban_time_format = cfg.get("weather","time_format")
strava_datetime_format = cfg.get("strava","date_time_format")

def merge_segments_and_weather():
    #iterate through segments in segment collection
    for segment in segments_collection.find():
        leaderboard_header = leaderboard_collection.find_one({},{'_id':0}).keys()#fieldnames of effort records
        weather_header = weather_collection.find_one({},{'_id':0}).keys()#fieldnames of weather records
        headers = leaderboard_header + weather_header#get the header of the dataframe

        df = None #dataframe for effort and weather data
        #get closest WBAN ID based on start location of segment
        location = [segment['start_latlng'][1],segment['start_latlng'][0]] #switch lng and lat records
        wban = db.command(SON([('geoNear', 'WBAN'),('near', location),('spherical','true')]))['results'][0]['obj']
        #iterate through each segment effort, get weather observation for that WBAN and time
        for effort in leaderboard_collection.find({'segment_id':segment['id']}):
            strava_time = dt.datetime.strptime(effort['start_date'],strava_datetime_format)
            date = dt.datetime.strftime(strava_time,wban_date_format)
            hour = dt.datetime.strftime(strava_time,'%H')
            #use regular expression of hourly_record _id field for query
            weather_id_re = re.compile(r'%s_%s_%s[\d]{2}' % (wban['WBAN_ID'],date,hour))
            weather_obs = weather_collection.find_one({'_id':weather_id_re})
            #create CSV row
            row = {}
            #merge effort and weather data in new row
            for key in effort:
                if key <> '_id': row[key]=effort[key]
            if weather_obs:
                for key in weather_obs:
                    if key <> '_id': row[key]=effort[key]
            else:
                for header in weather_header: row[header] = 'NA' #if no weather observation insert NA values
            if not df:
                df = pandas.DataFrame(row.items())
                #df.index = df['effort_id']
            else:
                df.append(row)


def main():

    #merge_segments_and_weather()

#map reduce functions
    count_mapper = Code("function () {emit(this.segment_id,1)}")
    count_reduce = Code("function (key,values) {return Array.sum(values)}")
##
    avg_speed_mapper = Code("function () {emit(this.segment_id,{time:this.moving_time,count:1})}")
    avg_speed_reduce = Code("function(key, valObj) {\
                                                     reducedVal = { time: 0, count: 0 };\
                                                     for (var idx = 0; idx < valObj.length; idx++) {\
                                                         reducedVal.time += valObj[idx].time;\
                                                         reducedVal.count += valObj[idx].count;\
                                                     }\
                                                     return reducedVal;\
                                                  };")

    avg_speed_finalize = Code("function (key, reducedVal) {\
                                                           reducedVal.avg = reducedVal.time/reducedVal.count;\
                                                           return reducedVal;\
                                                        };")

##
    result = leaderboard_collection.map_reduce(avg_speed_mapper, avg_speed_reduce,"avg_speed_results",finalize=avg_speed_finalize)
    for doc in result.find():
        pprint.pprint(doc)

