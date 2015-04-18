#-------------------------------------------------------------------------------
# Name:        Lat/lon math functions
# Purpose:     Functions for performing lat/long calculations
#
# Author:      Katherine Shelley
#
# Created:     4/2/2015
#-------------------------------------------------------------------------------

import math
earthRad = 40075000/2*math.pi #radius of earth in meters
earthRadMiles = 24902/2*math.pi #radius of earth in statute miles

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


def convert_lat_lon_strings(string):
    #cleans a latitude or longitude text string into decimal degrees
    from string import punctuation
    for symbol in punctuation.replace('-','').replace('.',''):
        string = string.replace(symbol,' ') #replace punctuation (other than - and .) with space
    coord_list = string.split()
    hemisphere = 1 #multiplier for converting hemispheres
    if coord_list[-1] == 'N' or coord_list[-1] == 'S' or coord_list[-1] == 'E' or coord_list[-1] == 'W':
        if coord_list[-1] == "S" or coord_list[-1] == "W":
            #if the coordinate is in the southern or western hemisphere, the lat/lon is negative.
            hemisphere = -1
            if coord_list[0].find('-') == -1: coord_list[0].replace('-','')#remove the negative symbol if it exists (sign-change will take place in DMS conversion)
        coord_list.pop()#remove the hemisphere indicator
    coordinate = 0
    denominator = 1
    for i in range(len(coord_list)):
        #DMS to decimal formula: deg = D + M/60 + S/3600
        coordinate+=hemisphere*float(coord_list[i])/denominator
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
        return [0,0]

