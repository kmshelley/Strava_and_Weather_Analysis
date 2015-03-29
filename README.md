# W205_Final_Project

Steps to Run the Program
1. Make sure all required Python Modules (pymongo, etc) are installed
2. Go inside the Root Dir: W205_Final_Project
3. python -m strava.main


3/7/2015:
Katherine Shelley
1. Updated config.py; removed Strava API key, added search grid mesh variable (in meters).
2. Updated strava_data_acquire_store.py to iterate through zip codes in config.py, accounted for empty leaderboard exception.
3. Updated SearchGrid.py to generate Strava API search parameter string.
4. Added google_polyline_encoder.py to ./report for decoding Strava polylines.

3/18/2015:
Katherine Shelley
1. Changed one line in 'strava_acquire' code: changed '_segment_id' field in leaderboards collection to be the Strava segment id, not the Mongo ID, feel free to change that back. It was giving me problems in Mongo for some reason.
2. Added weather acquire code.
3. Added testing/debugging code I have been using. It has some initial viz code snippets we may want to use later.


3/28/2015:
Katherine Shelley
1. Updated config.py; added dictionary with state abbreviations. I noticed that some Strava segments use abbreviations, others use full state name.
2. Updated weather_acquire.py with Mongo bulk insert operations, seems to have reduced time significantly (5 min per month vs. 15 min per month).
3. Added get_zip_codes.py, which develops a MongoDB collection of zip code data including state, county, zip code, and bounding box.
4. Included zip_code_database.csv downloaded from USPS.
