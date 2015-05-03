#-------------------------------------------------------------------------------
# Name:        User Interface
# Purpose:     Basic user interface for analyzing Strava/weather data
#
# Author:      Katherine Shelley
#
# Created:     4/15/2015
#-------------------------------------------------------------------------------
import sys
import subprocess
import os
import ast
from pymongo import MongoClient,GEO2D
from util import log
from util.config import Config
from report.google_polyline_encoder import decode
import datetime as dt
import analyze.export_data_to_csv as export
import report.data_viz as data_viz
import analyze.top_queries as top
import analyze.dbstats as dbstats


logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_collection = db[cfg.get("mongo","coll_zip")]
wban_collection = db[cfg.get("mongo","coll_wban")]
weather_collection = db[cfg.get("mongo","coll_weather")]


def cls():
    print "\n" * 100

def get_user_input():
    response = raw_input(
    "Select reporting option:\n\n \
        1. Export CSV for specific segment(s).\n \
        2. Export CSV for all segments in zip code(s).\n \
        3. Export random sample CSV.\n \
        4. Create GoogleEarth heat maps.\n \
        5. Get top segments. \n \
        6. Get MongoDB Stats. \n \
        (Type 'Q' to quit.)\n\n")
    if response == '1' or response == '2' or response == '3' or response == '4' or response == '5' or response == "6":
        return response
    elif response.lower() == 'q':
        cls()
        print "Goodbye!"
        sys.exit()
    else:
        print "Invalid choice. Please select a number."
        return get_user_input()

def get_segment_id():
    #for option 1
    response = raw_input(
    "Please enter segment ID (comma separate mulitple IDs).\n \
    (Type 'Q' to quit.)\n\n")
    if response.lower() == 'q':
        cls()
        pass
    else:
        return response.split(',')

def get_segment_zip():
    #for option 2 and 4
    response = raw_input(
    "Please enter zip code (comma separate mulitple zip codes).\n \
    (Type 'Q' to quit.)\n\n")
    if response.lower() == 'q':
        cls()
        pass
    else:
        return response.split(',')

def get_n_m():
    #for option 3
    response = raw_input(
    "Please enter number of segments to sample from.\n \
    (Type 'Q' to quit.)\n\n")
    if response.lower() == 'q':
        cls()
        pass
    else:
        try:
            n = int(response)
        except:
            "Invalid entry. Please enter a number!"
            return get_n_m()
    response = raw_input(
    "Please enter number of leaderboard entries to sample.\n \
    (Type 'Q' to quit.)\n\n")
    if response.lower() == 'q':
        cls()
        pass
    else:
        try:
            m = int(response)
        except:
            "Invalid entry. Please enter a number!"
            return get_n_m()
    return [n,m]

def get_segment_list_from_zip_codes(zip_codes):
    segments = []
    for zip_code in zip_codes:
        try:
            bbox = ast.literal_eval(zip_collection.find_one({'zip':zip_code})['bbox'])
            for segment in segments_collection.find({'loc':{'$geoWithin':{'$box':[bbox[:2],bbox[2:]]}}}):
                segments.append(int(segment['id']))
        except:
            pass
    return segments


############################## Error handling ##################################

def interrupt(exception=None):
    if type(exception) == KeyboardInterrupt or type(exception) == SystemExit:
        print "Goodbye!"
        sys.exit()
    else:
        print "***ERROR: " + str(exception)

################################################################################


while True:
    try:
        ui = get_user_input()
        #call various functions based on user input
        if ui == '1':
            start = dt.datetime.now()
            segments = [int(segment) for segment in get_segment_id()]
            segment_query = {'segment_id':{'$in':segments}}
            print "Fetching segment data... \n\n"
            data = export.join_segment_and_weather(segment_query)
            print "Finished exporting segment and weather data to %s" % data
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
        if ui == '2':
            start = dt.datetime.now()
            segments = get_segment_list_from_zip_codes(get_segment_zip())
            segment_query = {'segment_id':{'$in':segments}}
            print "Fetching segment data... \n\n"
            data = export.join_segment_and_weather(segment_query)
            print "Finished exporting segment and weather data to %s" % data
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
        if ui == '3':
            start = dt.datetime.now()
            n,m = get_n_m()
            print "Fetching segment data... \n\n"
            data = export.random_segments_and_weather_to_csv(n,m)
            print "Finished exporting random sample to %s" % data
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
        if ui == '4':
            start = dt.datetime.now()
            segments = get_segment_list_from_zip_codes(get_segment_zip())
            segment_query = {'segment_id':{'$in':segments}}
            print "Fetching segment data... \n\n"
            data_viz.speed_data_viz(segment_query)
            print "\n\nFinished writing kml files to %s" % os.getcwd()
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
        if ui == '5':
            start = dt.datetime.now()
            print "Running Random Queries on Segments/Leaderboards etc... \n\n"
            top.run_queries()
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
        if ui == '6':
            start = dt.datetime.now()
            print "Fetching MongoDB DB & Collection Level Statistics... \n\n"
            dbstats.get_mongo_stats()
            print "Total runtime: %s\n\n\n" % (dt.datetime.now() - start)
    except Exception as e:
        interrupt(e)


