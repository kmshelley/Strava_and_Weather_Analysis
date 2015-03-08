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
