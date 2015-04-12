import sys
import datetime
import config
import os
import re
import requests
import datetime
import time
import json
import urllib
import urllib2
import StringIO
import ast
import pprint
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from ..util import log
from ..util.config import Config
from SearchGrid import SearchGrid

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_data_collection = db[cfg.get("mongo", "coll_zip")]


# Strava Tokens
strava_tokens = []
total_strava_calls = 0

MIN_1 = 60
MIN_2 = 2 * MIN_1

# Following zips/segments were excluded due to Exceptions during Data Collection
missed_segments = []
missed_zips = []


def explore_segments(parameters):
    logger.info('Exploring segments for bounds: {0}'.format(parameters))

    res = None
    success = False

    while not success:
        try:
            res = requests.get(config.STRAVA_API_SEGMENT_EXPLORE_URI, headers=get_strava_header(), params=parameters)

            if res.status_code != 200 or "errors" in res.json():
                pprint.pprint(res.json())
                time.sleep(MIN_2)
                missed_segments.append(segment_id)
                continue
            success = True # Come out of the loop
        except Exception as e:
            logger.exception("### Exception exploring segments: {0}".format(parameters))
            time.sleep(MIN_2)
            continue

    #print pprint.pprint(res.json())
    segments = res.json()
    for segment in segments['segments']:
        yield segment


def get_zip():
    logger.info("Calling generator for zip codes which start with 9 [CA]")
    regx = re.compile("^9.*")
    zip_cursor = zip_data_collection.find({"zip": regx}).sort("zip", -1)
    # Store the cursor into a list to avoid timeouts by keeping the cursor open for a long time
    zip_list = []
    for z in zip_cursor:
        zip_list.append(z["zip"])
    for z in zip_list:
        yield z


def get_strava_header():
    global total_strava_calls
    if len(strava_tokens) == 0:  # First Time
        if cfg.get("strava", "access_token_1"):
            strava_tokens.append(cfg.get("strava", "access_token_1"))
        if cfg.get("strava", "access_token_2"):
            strava_tokens.append(cfg.get("strava", "access_token_2"))

    if len(strava_tokens) == 0:
        sys.exit("@@@@@ No Strava Tokens found in config, aborting data collection!!!")

    total_strava_calls += 1
    return {"Authorization": "Bearer %s" % strava_tokens[total_strava_calls % len(strava_tokens)]}


def fetch_store_segment_and_leaderboards():
    # Add unique keys
    segments_collection.ensure_index("id", unique=True)
    leaderboard_collection.ensure_index("effort_id", unique=True)

    start = datetime.datetime.now()

    #KMS: Iterate through the zip codes, compiling leaderboard and segment data
    for zipcode in get_zip():

        try:
            #generate a search grid for the zip code
            bbox = zip_data_collection.find_one({'zip': str(zipcode)})['bbox']
            if bbox and bbox is not None:
                bbox = ast.literal_eval(bbox)
            else:
                logger.info("Bounding Box: {0}".format(bbox))
                bbox = [0, 0, 0, 0]

            #logger.info("Bounding Box: " + str(bbox))
            grid = SearchGrid(bbox)
            for parameters in grid.define_strava_params():
                for segment in explore_segments(parameters):
                    segment_id = segment["id"]
                    logger.info('ZIP [{0}] Fetching segment: {1}'.format(zipcode, segment_id))
                    res = requests.get(config.STRAVA_API_SEGMENT_URI % segment_id, headers=get_strava_header())

                    try:
                        if res.status_code != 200 or "errors" in res.json():
                            pprint.pprint(res.json())
                            time.sleep(MIN_2)
                            missed_segments.append(segment_id)
                            continue
                    except Exception as e:
                        logger.exception("### Exception fetching segments: {0}".format(segment_id))
                        time.sleep(MIN_2)
                        missed_segments.append(segment_id)
                        continue

                    # MongoDB ID
                    _id = None
                    try:
                        #Insert Segment into MongoDB
                        _id = segments_collection.insert(res.json())
                    except pymongo.errors.DuplicateKeyError as dk:
                        logger.exception("### Exception inserting segment: {0}".format(dk))
                        #Get Segment ID from DB
                        _id = segments_collection.find_one({'id': segment_id})["_id"]
                        # This segment has already been processed earlier
                        #continue
                    except Exception as e:
                        #any other exception
                        logger.exception("### Exception inserting segment: {0}".format(e))
                        missed_segments.append(segment_id)
                        continue

                    logger.info("DB Unique Key: {0}".format(_id))

                    num_athletes = res.json()['athlete_count']
                    logger.info('Athlete Count: {0}'.format(num_athletes))

                    page_num = 1
                    entry_num = 0;

                    while num_athletes > 0 and page_num < 2 + num_athletes / config.STRAVA_PAGE_LIMIT:
                        leaderboard_batch = []
                        logger.info("[Segment:{0}] Fetching Leader-board Page: {1}".format(segment_id, page_num))


                        res = requests.get(config.STRAVA_API_SEGMENT_LEADERBOARD_URI % segment_id,
                                           headers=get_strava_header(),
                                           params={'per_page': config.STRAVA_PAGE_LIMIT, 'page': page_num})
                        '''
                        res = requests.get(config.STRAVA_API_SEGMENT_ALL_EFFORTS_URI % segment_id,
                                           headers=get_strava_header())
                        '''

                        #pprint.pprint(res.json())

                        try:
                            if res.status_code != 200 or "errors" in res.json():
                                pprint.pprint(res.json())
                                logger.info("Sleeping after Requesting Retry for Page: {0}".format(page_num))
                                time.sleep(MIN_2)
                                continue
                        except Exception as e:
                            logger.exception("### Exception fetching Leaderboard Entries for Segment: {0}".format(segment_id))
                            logger.info("Sleeping after Exception for Page: {0}".format(page_num))
                            time.sleep(MIN_2)
                            continue

                        page_num += 1

                        # Initialize Bulk Op
                        bulk = leaderboard_collection.initialize_unordered_bulk_op()

                        for entry in res.json()['entries']:
                            #pprint.pprint(entry)
                            entry_num += 1
                            entry["segment_id"] = segment_id  # try using strava segment id instead of MongoDB id
                            entry["_segment_id"] = _id  # this is inserting as 'null' in MongoDB
                            leaderboard_batch.append(entry)
                            bulk.insert(entry)
                        #if len(leaderboard_batch) == 0:
                        #    print "Empty leaderboard!"

                        try:
                            #ids = leaderboard_collection.insert(leaderboard_batch)
                            #print(ids)

                            #Insert Efforts Batch into MongoDB
                            result = bulk.execute()
                            pprint.pprint(result)
                            logger.info("[Segment:%s][Total:%d] Total Number of Leaderboard Entries inserted into Mongo is %d",
                                                        segment_id, num_athletes, entry_num)
                        except BulkWriteError as bwe:
                            logger.exception("### BulkWriteError inserting leaderboard: {0}".format(bwe))
                            #pprint.pprint(bwe.details) # Commented out: Prints out a big JSON
                        except pymongo.errors.DuplicateKeyError as dk:
                            logger.exception("### Exception inserting leaderboard: %s", dk)
                            logger.info("[Segment:%s][Total:%d] Total Number of Leaderboard Entries inserted into Mongo is %d",
                                                        segment_id, num_athletes, len(ids))
                        except Exception as e:
                            #any other exception
                            logger.exception("### Exception inserting leaderboard: %s", e)
                            logger.info("[Segment:%s][Total:%d] Total Number of Leaderboard Entries inserted into Mongo is %d",
                                                        segment_id, num_athletes, len(ids))
        except Exception as e:
            #any exception while processing zip
            logger.exception("### Exception processing zip: %s", str(zipcode))
            missed_zips.append(str(zipcode))
            continue

    print("@@@@@ Missed Zips @@@@\n")
    print(missed_zips)
    print("\n\n")
    print("@@@@@ Missed Segments @@@@\n")
    print(missed_segments)

    print "Finished acquiring data: \n \
            Total Segments: " + str(segments_collection.count()) + "\n \
            Total Leaderboards: " + str(leaderboard_collection.count()) + "\n"

    print "Total acquisition time: " + str(datetime.datetime.now() - start)

if __name__ == '__main__':
    """Test"""
    logger.info("Invoking Main...")
    '''
    for z in get_zip():
        print(z)
    '''
    for i in xrange(100):
        get_strava_header()
