#-------------------------------------------------------------------------------
# Name:        User Interface
# Purpose:     Basic user interface for analyzing Strava/weather data
#
# Author:      Katherine Shelley
#
# Created:     4/15/2015
#-------------------------------------------------------------------------------
import sys, os
from pymongo import MongoClient,GEO2D
from util import log
from util.config import Config
from report.google_polyline_encoder import decode
import simplekml
import csv
import pandas
import datetime as dt


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




def get_user_input():
    response = raw_input(
    "Select reporting option:\n\n \
        1. Option 1\n \
        2. Option 2\n \
        3. Option 3\n \
        (Type 'Q' to quit.)\n\n")
    if response == '1' or response == '2' or response == '3':
        return response
    elif response.lower() == 'q':
        print "Goodbye!"
        sys.exit()
    else:
        print "Invalid choice. Please select a number."
        return get_user_input()


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
    except Exception as e:
        interrupt(e)
            

