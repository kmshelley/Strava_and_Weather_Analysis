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
import os
import sys
import subprocess
from pymongo import MongoClient, GEOSPHERE
from bson import SON
from google_polyline_encoder import decode
import datetime as dt
import pprint
import simplekml
import math
import ast
import copy
from util import lat_lng
from util.config import Config
from analyze import export_data_to_csv as export

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_data_collection = db[cfg.get("mongo", "coll_zip")]
weather_collection = db[cfg.get("mongo","coll_weather")]
wban_collection = db[cfg.get("mongo","coll_wban")]

def make_kml_heat_maps(segments):
    #creates heat map based on polylines and relative counts
    avg_speed_kml = simplekml.Kml()
    max_speed_kml = simplekml.Kml()
    segments_kml = simplekml.Kml()
    error_count=0
    for seg in segments:
        try:
            avg_line = avg_speed_kml.newlinestring(name = str(seg),coords=segments[seg]['avg_point'])
            avg_line.altitudemode = simplekml.AltitudeMode.relativetoground
            avg_line.extrude = 1
            avg_line.style.linestyle.color = '7fffffff'#transparent white
            avg_line.style.linestyle.colormode='random' #creates random colors for lines
            avg_line.style.linestyle.width = 50


            max_line = max_speed_kml.newlinestring(name = str(seg),coords=segments[seg]['max_point'])
            max_line.altitudemode = simplekml.AltitudeMode.relativetoground
            max_line.extrude = 1
            max_line.style.linestyle.color = '7fffffff'#transparent white
            max_line.style.linestyle.colormode='random' #creates random colors for lines
            max_line.style.linestyle.width = 50

            segment_line = segments_kml.newlinestring(name=str(seg),coords=segments[seg]['line'])
            segment_line.altitudemode = simplekml.AltitudeMode.relativetoground
            segment_line.extrude = 1
            segment_line.style.linestyle.color = '7fffffff'#transparent white
            segment_line.style.linestyle.colormode='random' #creates random colors for lines
            segment_line.style.linestyle.width = 10
        except:
            error_count+=1
    print "Completed with %s errors." % error_count
    avg_speed_kml.save(os.path.join(os.getcwd(), 'avg_speed.kml'))
    max_speed_kml.save(os.path.join(os.getcwd(), 'max_speed.kml'))
    segments_kml.save(os.path.join(os.getcwd(), 'segments.kml'))

def geo_index_segments():
    start = dt.datetime.now()

    bulk = segments_collection.initialize_unordered_bulk_op()
    for segment in segments_collection.find():
        coords = [segment['start_longitude'],segment['start_latitude']]
        bulk.find({'_id':segment['_id']}).update({'$set':{'loc':{'type':'Point','coordinates':coords}}})
    try:
        result = bulk.execute()
        pprint.pprint(result)
    except BulkWriteError as bwe:
        pprint.pprint(bwe.details)
    except Exception as e:
        print "#####ERROR: %s" % e

    print "Indexing coordinates..."
    try:
        segments_collection.ensure_index([('loc', GEOSPHERE)]) #create index on coordinates
    except Exception as e:
        print "#####ERROR: %s" % e

    print "Runtime: " + str(dt.datetime.now() - start)

def speed_data_viz(query={}):
    #returns a kml file for a heat map for n segments
    #near given coordinates (in lng/lat order)

    heatMap = {}
    total_size, total_avg = 0,0 #for normalizing heat map size
    leaders = export.export_segment(query)


    #****************Use mrjob to get average speed per segment****************
    consoleCmds = 'python .\\analyze\\mrjob_average_speed.py < %s' % (leaders)
    #print consoleCmds
    with open('output.txt','w') as output:
        p = subprocess.Popen(consoleCmds,cwd = os.getcwd(),stdout=output)
    p.wait() #wait for the command to finish

    with open('output.txt','r') as output:
        for line in output:
            try:
                _id = int(ast.literal_eval(line.split('\t')[0]))
                altitude = ast.literal_eval(line.split('\t')[1]) * 30.48#get the average speed
                segment = segments_collection.find_one({'id':_id})
                coords = segment['loc']['coordinates']
                #make a 3d line at the start lat/long with height equal to avg speed
                floor = tuple(copy.copy(coords)+[0])
                ceiling = tuple(copy.copy(coords)+[altitude])
                avg_pt_coords = [floor,ceiling]
                line_coords = decode(segment['map']['polyline'])

                heatMap[_id] = {'line':line_coords,'avg_point':avg_pt_coords} #altitude offset based on avg speed of the segment
            except Exception as e:
                print "ERROR: %s" % e

    #****************Use mrjob to get max speed per segment****************
    consoleCmds = 'python .\\analyze\\mrjob_max_speed.py < %s' % (leaders)
    #print consoleCmds
    with open('output.txt','w') as output:
        p = subprocess.Popen(consoleCmds,cwd = os.getcwd(),stdout=output)
    p.wait() #wait for the command to finish

    with open('output.txt','r') as output:
        for line in output:
            try:
                _id = int(ast.literal_eval(line.split('\t')[0]))
                altitude = ast.literal_eval(line.split('\t')[1])* 30.48 #get the average speed
                segment = segments_collection.find_one({'id':_id})
                coords = segment['loc']['coordinates']
                #make a 3d line at the start lat/long with height equal to max speed
                floor = tuple(copy.copy(coords)+[0])
                ceiling = tuple(copy.copy(coords)+[altitude])
                max_pt_coords = [floor,ceiling]

                heatMap[_id]['max_point'] = max_pt_coords #altitude offset based on max speed of the segment
            except Exception as e:
                print "ERROR: %s" % e
    make_kml_heat_maps(heatMap)
    os.remove('output.txt')
    os.remove(leaders)
