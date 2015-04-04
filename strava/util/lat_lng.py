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
