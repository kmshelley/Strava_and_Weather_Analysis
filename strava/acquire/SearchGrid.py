#lat/lon grid class
#import lat_lng
from ..util import lat_lng
import math, os, simplekml
earthRad = 40075000/2*math.pi #radius of earth in meters

class SearchGrid():
    #defines the address search grid
    def __init__(self,bbox=[0,0,0,0],resolution=5000):
        #self.zipCode = zipCode
        self.resolution = resolution #resolution of the grid squares (in meters)
        self.bbox = {'sw.lat': bbox[1],'sw.lng':bbox[0],'ne.lat':bbox[3],'ne.lng':bbox[2]}#bounding box
        #self.googleBB = get_address_coordinates(zipCode) #Google Maps API returned bounding coordinates of zip code
        #characteristics of the grid
        self.diagonal_dist = lat_lng.dist_lat_lon(self.bbox['sw.lat'],self.bbox['sw.lng'],self.bbox['ne.lat'],self.bbox['ne.lng']) #distance of diagonal between bounding coordinates
        self.diagonal_bearing = lat_lng.bearing_from_two_lat_lons(self.bbox['sw.lat'],self.bbox['sw.lng'],self.bbox['ne.lat'],self.bbox['ne.lng']) #bearing between bounding coordinates
        self.side_length = math.sqrt((self.diagonal_dist**2)/2) #width of bounding box (KM)
        #other bounding points of the box
        nw_lat,nw_lng = lat_lng.lat_lon_from_point_and_bearing(self.bbox['sw.lat'],self.bbox['sw.lng'],self.diagonal_bearing - 45,self.side_length)
        se_lat,se_lng = lat_lng.lat_lon_from_point_and_bearing(self.bbox['sw.lat'],self.bbox['sw.lng'],self.diagonal_bearing + 45,self.side_length)

        #dictionary defining the points of the entire bounding box
        self.bounding_box = {
            'sw.lat': self.bbox['sw.lat'],
            'sw.lng': self.bbox['sw.lng'],
            'se.lat': se_lat,
            'se.lng': se_lng,
            'nw.lat': nw_lat,
            'nw.lng': nw_lng,
            'ne.lat': self.bbox['ne.lat'],
            'ne.lng': self.bbox['ne.lng']
            }

    def bounding_box_kml(self):
        #Creates a Google Earth KML of the address bounding-box
        kml = simplekml.Kml()
        coords = [
                (self.bounding_box['se.lng'],self.bounding_box['se.lat']),
                (self.bounding_box['sw.lng'],self.bounding_box['sw.lat']),
                (self.bounding_box['nw.lng'],self.bounding_box['nw.lat']),
                (self.bounding_box['ne.lng'],self.bounding_box['ne.lat']),
                (self.bounding_box['se.lng'],self.bounding_box['se.lat'])
                ]
        kml.newlinestring(name='bouding_box', description='bounding box',
                                coords=coords)
        kml.save(os.getcwd() + "\\bb.kml")

    def grid_walk(self):
        #generator function for grid points
        #need to make more robust for crossing hemispheres
        steps = int(self.side_length/self.resolution) + 1 #number of grid squares per side (+ 1 provides a little overlap for completeness)

        lat,lng = self.bounding_box['sw.lat'],self.bounding_box['sw.lng'] #start the walk at the southwest corner
        lat1,lng1 = lat,lng #sw point of grid square
        for i in range(steps):
            for j in range(steps):
                lat2,lng2 = lat_lng.lat_lon_from_point_and_bearing(lat1,lng1,self.diagonal_bearing, math.sqrt(2 * self.resolution**2)) #ne point of grid square
                center_lat,center_lng = lat_lng.lat_lon_from_point_and_bearing(lat1,lng1,self.diagonal_bearing, math.sqrt(2 * self.resolution**2)/2) #center point of grid square
                grid_square = {
                    'sw.lat': lat1,
                    'sw.lng': lng1,
                    'ne.lat': lat2,
                    'ne.lng': lng2,
                    'center.lat': center_lat,
                    'center.lng': center_lng
                    }
                yield grid_square
                lat1,lng1 = lat_lng.lat_lon_from_point_and_bearing(lat1,lng1,self.diagonal_bearing + 45, self.resolution) #move <resolution> meters to the east
            lat,lng = lat_lng.lat_lon_from_point_and_bearing(lat,lng,self.diagonal_bearing - 45, self.resolution) #move starting point og grid walk <resolution> meters north
            lat1,lng1 = lat,lng #reset lat1, lng1

    def define_strava_params(self):
        walk = self.grid_walk()
        PARAMS = {}
        try:
            while True:
                points = walk.next()
                PARAMS = {"bounds": str(points['sw.lat']) + "," + str(points['sw.lng']) + "," + str(points['ne.lat']) + "," + str(points['ne.lng'])}
                yield PARAMS
        except StopIteration:
            pass

    def print_grid(self):
        #prints the grid walk
        walk = self.grid_walk()
        try:
            while True:
                print walk.next()
        except StopIteration:
            pass

    def grid_kml(self):
        #creates a Google Earth KML file of the grid points
        walk = self.grid_walk()
        kml = simplekml.Kml()
        index = 0
        try:
            while True:
                index+=1
                points = walk.next()

                coords = [(points['sw.lng'],points['sw.lat'])]
                pnt = kml.newpoint(name='', coords=coords)
                pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'

                coords = [(points['center.lng'],points['center.lat'])]
                pnt = kml.newpoint(name='', coords=coords)
                pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'

                coords = [(points['ne.lng'],points['ne.lat'])]
                pnt = kml.newpoint(name='', coords=coords)
                pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'
        except StopIteration:
            pass
        kml.save(os.getcwd() + "\\grid_" + str(self.zipCode) + "_" + str(self.resolution) + "_meters.kml")



