import config
import os
import sys
import requests
import datetime
import time
import json
import urllib
import urllib2
import StringIO
import pprint
import pymongo
from pymongo import MongoClient

# MongoDB Client & DB
client = MongoClient('mongodb://localhost:27017/')
db = client['strava']

PARAMS = {"bounds": "37.695010" + "," + "-122.510605" + "," + "37.815531" + "," + "-122.406578"}


def explore_segments():
    print('Exploring segments for bounds: {0}'.format(PARAMS))
    res = requests.get(config.STRAVA_API_SEGMENT_EXPLORE_URI, headers=config.STRAVA_API_HEADER, params=PARAMS)

    #print pprint.pprint(res.json())
    segments = res.json()
    for segment in segments['segments']:
        yield segment


def fetch_store_segment_and_leaderboards():
    segments_collection = db['segments']
    leaderboard_collection = db['leaderboards']

    for segment in explore_segments():
        print('Fetching segment: {0}'.format(segment["id"]))
        res = requests.get(config.STRAVA_API_SEGMENT_URI % segment["id"], headers=config.STRAVA_API_HEADER)

        _id = None
        try:
            #Insert Segment into MongoDB
            _id = segments_collection.insert(res.json())
        except pymongo.errors.DuplicateKeyError as dk:
            print("### Exception inserting segment: ", dk)
            #Get Segment ID from DB
            _id = segments_collection.find_one({'id': segment["id"]})["_id"]
            # This segment has already been processed earlier
            #continue

        print(_id)

        num_athletes = res.json()['athlete_count']
        print('Athlete Count: {0}'.format(num_athletes))

        page_num = 1

        while page_num < 2 + num_athletes / config.STRAVA_PAGE_LIMIT:
            leaderboard_batch = []
            print("Leader-board Request for Page: ", page_num)


            res = requests.get(config.STRAVA_API_SEGMENT_LEADERBOARD_URI % segment["id"],
                               headers=config.STRAVA_API_HEADER,
                               params={'per_page': config.STRAVA_PAGE_LIMIT, 'page': page_num})
            '''
            res = requests.get(config.STRAVA_API_SEGMENT_ALL_EFFORTS_URI % segment["id"],
                               headers=config.STRAVA_API_HEADER)
            '''

            #pprint.pprint(res.json())

            try:
                if res.status_code != 200 or "errors" in res.json():
                    pprint.pprint(res.json())
                    print("Sleeping after Requesting Retry for Page: ", page_num)
                    time.sleep(60)
                    continue
            except Exception as e:
                print(res)
                print("### Exception: ", e)
                print("Sleeping after Exception for Page: ", page_num)
                time.sleep(60*5)
                continue

            page_num += 1

            for effort in res.json():
                effort["_segment_id"] = _id
                leaderboard_batch.append(effort)
                #pprint.pprint(effort)

            try:
                #Insert Efforts Batch into MongoDB
                ids = leaderboard_collection.insert(leaderboard_batch)
                print(ids)
            except pymongo.errors.DuplicateKeyError as dk:
                print("### Exception inserting leaderboard: ", dk)


if __name__ == '__main__':
    fetch_store_segment_and_leaderboards()



