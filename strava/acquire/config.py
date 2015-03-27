STRAVA_ACCESS_TOKEN = "XXXXX"
STRAVA_API_URI = "https://www.strava.com/api/v3/"
STRAVA_API_HEADER = {"Authorization": "Bearer %s" % STRAVA_ACCESS_TOKEN}

STRAVA_API_SEGMENT_URI = STRAVA_API_URI + "segments/%d"
STRAVA_API_SEGMENT_ALL_EFFORTS_URI = STRAVA_API_SEGMENT_URI + "/all_efforts"
STRAVA_API_SEGMENT_LEADERBOARD_URI = STRAVA_API_SEGMENT_URI + "/leaderboard"
STRAVA_API_SEGMENT_EXPLORE_URI = STRAVA_API_URI + "segments/explore"

STRAVA_PAGE_LIMIT = 200

WUNDERGROUND_API_ACCESS_TOKEN = "XXXXX"

segment_ids = [646257]
zipcode = [95051,95050]
mesh = 50000 #search grid square width in meters

states = {
'California':'CA'
}
