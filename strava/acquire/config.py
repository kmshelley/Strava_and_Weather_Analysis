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
    'Armed Forces Americas':	'AA',
    'Armed Forces Europe':	'AE',
    'Alaska':	'AK',
    'Alabama':	'AL',
    'Armed Forces Pacific':	'AP',
    'Arkansas':	'AR',
    'Arizona':	'AZ',
    'California':	'CA',
    'Colorado':	'CO',
    'Connecticut':	'CT',
    'District of Columbia':	'DC',
    'Delaware':	'DE',
    'Florida':	'FL',
    'Georgia':	'GA',
    'Guam':	'GU',
    'Hawaii':	'HI',
    'Iowa':	'IA',
    'Idaho':	'ID',
    'Illinois':	'IL',
    'Indiana':	'IN',
    'Kansas':	'KS',
    'Kentucky':	'KY',
    'Louisiana':	'LA',
    'Massachusetts':	'MA',
    'Maryland':	'MD',
    'Maine':	'ME',
    'Michigan':	'MI',
    'Minnesota':	'MN',
    'Missouri':	'MO',
    'Mississippi':	'MS',
    'Montana':	'MT',
    'North Carolina':	'NC',
    'North Dakota':	'ND',
    'Nebraska':	'NE',
    'New Hampshire':	'NH',
    'New Jersey':	'NJ',
    'New Mexico':	'NM',
    'Nevada':	'NV',
    'New York':	'NY',
    'Ohio':	'OH',
    'Oklahoma':	'OK',
    'Oregon':	'OR',
    'Pennsylvania':	'PA',
    'Puerto Rico':	'PR',
    'Rhode Island':	'RI',
    'South Carolina':	'SC',
    'South Dakota':	'SD',
    'Tennessee':	'TN',
    'Texas':	'TX',
    'Utah':	'UT',
    'Virginia':	'VA',
    'Virgin Islands':	'VI',
    'Vermont':	'VT',
    'Washington':	'WA',
    'Wisconsin':	'WI',
    'West Virginia':	'WV',
    'Wyoming':	'WY'
    }
