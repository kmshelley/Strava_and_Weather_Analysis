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
from pymongo import MongoClient, GEO2D, GEOSPHERE
import datetime as dt
import pprint
import simplekml
import math
import os

earthRad = 40075000/2*math.pi #radius of earth in meters
earthRadMiles = 3963.2


def convert_lat_lon_strings(string):
    #cleans a latitude or longitude text string into decimal degrees
    from string import punctuation
    for symbol in punctuation.replace('-','').replace('.',''):
        string = string.replace(symbol," ") #replace punctuation (other than - and .) with space
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
        if text == '32.267N 64.667W':
            print "DEBUG!!!"
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



############################ Lat/Lon formulas ########################################
def dist_lat_lon(latCoord1, lonCoord1, latCoord2, lonCoord2):
    # Distance between two points, (lat1,lon1) and (lat2,lon2)
    distance = 0 #reset the distance calculation
    latRad1 = math.radians(latCoord1) #convert the first latitude to radians
    lonRad1 = math.radians(lonCoord1) #convert the first longitude to radians
    latRad2 = math.radians(latCoord2) #convert the second latitude to radians
    lonRad2 = math.radians(lonCoord2) #convert the second longitude to radians
    distance = earthRad * math.atan2(math.sqrt((math.cos(latRad2)*math.sin(lonRad1 - lonRad2))**2+(math.cos(latRad1)*math.sin(latRad2)-math.sin(latRad1)*math.cos(latRad2)*math.cos(lonRad1-lonRad2))**2),(math.sin(latRad2)*math .sin(latRad1)+math.cos(latRad1)*math.cos(latRad2)*math.cos(lonRad1-lonRad2)))
    return distance

def lat_lon_from_point_and_bearing(lat,lon,angle,dist):
    #returns a lat/lon pair that is dist NM from given lat/lon at the given angle bearing
    lat2  = math.degrees(math.asin(math.sin(math.radians(lat))*math.cos(dist/earthRad) + math.cos(math.radians(lat))*math.sin(dist/earthRad)*math.cos(math.radians(angle))))
    lon2 = lon + math.degrees(math.atan2(math.sin(math.radians(angle))*math.sin(dist/earthRad)*math.cos(math.radians(lat)),math.cos(dist/earthRad) - math.sin(math.radians(lat))*math.sin(math.radians(lat2))))
    return lat2, lon2

def bearing_from_two_lat_lons(lat1,lon1,lat2,lon2):
    x = math.sin(math.radians(lon2)-math.radians(lon1))*math.cos(math.radians(lat2))
    y = math.cos(math.radians(lat1))*math.sin(math.radians(lat2)) - math.sin(math.radians(lat1))*math.cos(math.radians(lat2))*math.cos(math.radians(lon2)-math.radians(lon1))
    return (math.degrees(math.atan2(x,y))+360)%360

def find_midpoint_between_lat_lons(lat1,lon1,lat2,lon2):
    return lat_lon_from_point_and_bearing(lat1,lon1,bearing_from_two_lat_lons(lat1,lon1,lat2,lon2),dist_lat_lon(lat1,lon1,lat2,lon2)/2)
########################################################################################


def main():
    start = dt.datetime.now()


    client = MongoClient()

    db = client['noaa_weather']
    obs_coll = db['hourly_coll']
    wban_coll = db['WBAN']
    geo_coll = db['geo_json_coll']
    zips = client['zip_codes']
    zip_data_coll = zips['zip_data_coll']


##    bulk = geo_coll.initialize_ordered_bulk_op()
##    print len(list(wban_coll.find()))
##    for WBAN in wban_coll.find({}):
##        coords = clean_lat_long(WBAN['LOCATION']) #get cleaned lat/lng coordinates
##        _id = WBAN['_id'] #keep parent ID
##        if len(list(geo_coll.find({'_id':_id}))) == 0:
##            record = {'_id':_id,'type':'Point','coordinates':coords}
##            bulk.insert(record)
##    try:
##        result = bulk.execute()
##        pprint.pprint(result)
##    except Exception as e:
##        print "#####ERROR: %s" % e

    print "Indexing coordinates..."
    try:
        geo_coll.ensure_index([('coordinates', GEO2D)]) #create index on coordinates
    except Exception as e:
        print "#####ERROR: %s" % e

    kml = simplekml.Kml()

    for wban in wban_coll.find({'STATION_NAME':'SAN FRANCISCO'}):
        doc = geo_coll.find_one({'_id':wban['_id']})
####        print doc
##        if doc['coordinates'] == [0,0]:
##            print "No coordinates for %s" % wban['WBAN_ID']
##        else:
##            pnt = kml.newpoint(name = '',coords=[(doc['coordinates'][0],doc['coordinates'][1])])
##            pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
##            pnt.style.iconstyle.color = 'red'
##    kml.save(os.getcwd() + "\\wbans_GEOJSON_SF.kml")
##    kml = None



    coords = eval(zip_data_coll.find_one({'zip':'95014'})['bbox'])#define coordinates of zip-code polygon

    center = find_midpoint_between_lat_lons(coords[1],coords[0],coords[3],coords[2])
    kml = simplekml.Kml()

    #for doc in geo_coll.find({'coordinates':{'$geoWithin':{'$centerSphere': [[center[1],center[0]], 50/3963.2 ]}}}):
    for doc in geo_coll.find({'coordinates':{'$near':[center[1],center[0]]}}).limit(10):
        #print doc
        wban_name = wban_coll.find_one({'_id':doc['_id']})['STATION_NAME']
        pnt = kml.newpoint(name = wban_name,coords=[(doc['coordinates'][0],doc['coordinates'][1])])
        pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
        pnt.style.iconstyle.color = '7fff0000'#transparent blue
    kml.save(os.getcwd() + "\\wbans_near_95014.kml")

    print "Runtime: " + str(dt.datetime.now() - start)

if __name__ == '__main__':
    main()
