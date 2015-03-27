#-------------------------------------------------------------------------------
# Name:        Strava debug scratch pad and testing
# Purpose:
#
# Author:      Katherine
#
# Created:     03/04/2015
#-------------------------------------------------------------------------------
import sys, os, pymongo
##sys.path.append('C:\\Users\\Katherine\\Documents\\GitHub\\W205_Final_Project\\strava\\acquire')
##sys.path.append('C:\\Users\\Katherine\\Documents\\GitHub\\W205_Final_Project\\strava\\report')


from SearchGrid import SearchGrid
from strava_data_acquire_store import explore_segments
from google_polyline_encoder import decode
import simplekml, config, pymongo
from pymongo import MongoClient
import datetime
import csv

def main():
##    start = datetime.datetime.now()
##    #Check segment explorer
##    #f = open(os.getcwd() + "/temp_test_" + str(config.mesh) + "m.csv",'w')
##    #f.write("Zip Code,Segment ID,\n")
##    #KMS: Iterate through the zip codes, compiling leaderboard and segment data
##    kml = simplekml.Kml()
##
##    searched = []
##    for zipcode in config.zipcode:
##        #generate a search grid for the zip code
##        grid = SearchGrid(zipcode,config.mesh)
##        grid.grid_kml() #DEBUG
##        for parameters in grid.define_strava_params():
##            for segment in explore_segments(parameters):
##                if segment['id'] not in searched:
##                    coords = decode(segment['points'])
##                    ls = kml.newlinestring(name=str(segment['id']))
##                    ls.coords = coords
##                    ls.extrude = 1
##                    ls.altitudemode = simplekml.AltitudeMode.relativetoground
##                    ls.style.linestyle.width = 2
##                    ls.style.linestyle.color = simplekml.Color.yellowgreen
##                #f.write(str(zipcode)+ "," + str(segment['id']) + ",\n")
##        kml.save(os.getcwd() + "\\kml_" + str(zipcode) + "_" + str(config.mesh) + "m.kml")
##        print "saved"
##    #f.close()

    #Check contents of MongoDB
    # MongoDB Client & DB
    client = MongoClient('mongodb://localhost:27017/')
    print client.database_names()

    db = client['strava']
    print db.collection_names()

    segments_collection = db['segments']
    leaderboard_collection = db['leaderboards']

    print "Segments: " + str(segments_collection.count())
    print "Leaderboards: " + str(leaderboard_collection.count())

    #get leaderboard data into CSV
    segments = db['segments']
    print list(segments.find_one({}))
    most_popular = list(segments.find({'activity_type':'Ride'}).sort('effort_count',pymongo.DESCENDING).limit(1))[0]['id']
    leaders = db['leaderboards']
    with open('leaders.csv', 'w') as csvfile:
        fieldnames = leaders.find_one({},{'_id':0}).keys()#get the header of the csv
        print fieldnames
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in leaders.find({'_segment_id':most_popular},{'_id':0}).sort('moving_time',pymongo.ASCENDING).limit(50): #find the top 50 fastest efforts
            writer.writerow(row)

    #get points for the top X most popular segments, make GoogleEarth KML
    top_val = 50
    topSegments = list(segments.find({'activity_type':'Ride'}).sort('effort_count',pymongo.DESCENDING).limit(top_val))
    size = 0
    #get the total effort count for all segments
    for segment in topSegments:
        size+= segment['athlete_count']
    kml = simplekml.Kml()
    for segment in topSegments:
        pnt = kml.newpoint(name = '',coords=[(segment['start_longitude'],segment['start_latitude'])])
        pnt.style.iconstyle.scale = (segment['athlete_count']+0.0)/size * 100 #adjust the size based on percentage of popularity
        pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
        pnt.style.iconstyle.color = '7fff0000'#transparent blue
    kml.save(os.getcwd() + "\\top_" + str(top_val) + ".kml")


if __name__ == '__main__':
    main()
