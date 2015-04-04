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
import pprint
import pymongo
from pymongo import MongoClient
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

#PARAMS = {"bounds": "37.695010" + "," + "-122.510605" + "," + "37.815531" + "," + "-122.406578"}


def explore_segments(parameters):
    logger.info('Exploring segments for bounds: {0}'.format(parameters))
    res = requests.get(config.STRAVA_API_SEGMENT_EXPLORE_URI, headers=config.STRAVA_API_HEADER, params=parameters)

    #print pprint.pprint(res.json())
    segments = res.json()
    for segment in segments['segments']:
        yield segment


def get_zip():
    logger.info("Calling generator for zip codes which start with 9 [CA]")
    regx = re.compile("^9.*")
    for z in zip_data_collection.find({"zip": regx}):
        yield z["zip"]


def fetch_store_segment_and_leaderboards():
    segments_collection.ensure_index("id", unique=True)
    leaderboard_collection.ensure_index("effort_id", unique=True)

    start = datetime.datetime.now()

    #KMS: Iterate through the zip codes, compiling leaderboard and segment data
    for zipcode in get_zip():
        #generate a search grid for the zip code
        bbox = eval(zip_data_collection.find_one({'zip': str(zipcode)})['bbox'])
        grid = SearchGrid(bbox)
        for parameters in grid.define_strava_params():
            for segment in explore_segments(parameters):
                segment_id = segment["id"]
                logger.info('ZIP [{0}] Fetching segment: {1}'.format(zipcode, segment_id))
                res = requests.get(config.STRAVA_API_SEGMENT_URI % segment_id, headers=config.STRAVA_API_HEADER)

                _id = None
                try:
                    #Insert Segment into MongoDB
                    _id = segments_collection.insert(res.json())
                except pymongo.errors.DuplicateKeyError as dk:
                    logger.info("### Exception inserting segment: %s", dk)
                    #Get Segment ID from DB
                    _id = segments_collection.find_one({'id': segment_id})["_id"]
                    # This segment has already been processed earlier
                    #continue
                except Exception as e:
                    #any other exception
                    logger.info("### Exception inserting segment: %s", e)

                logger.info("DB Unique Key %s", _id)

                num_athletes = res.json()['athlete_count']
                logger.info('Athlete Count: {0}'.format(num_athletes))

                page_num = 1
                entry_num = 0;

                while page_num < 2 + num_athletes / config.STRAVA_PAGE_LIMIT:
                    leaderboard_batch = []
                    logger.info("[Segment:{0}] Fetching Leader-board Page: {1}".format(segment_id, page_num))


                    res = requests.get(config.STRAVA_API_SEGMENT_LEADERBOARD_URI % segment_id,
                                       headers=config.STRAVA_API_HEADER,
                                       params={'per_page': config.STRAVA_PAGE_LIMIT, 'page': page_num})
                    '''
                    res = requests.get(config.STRAVA_API_SEGMENT_ALL_EFFORTS_URI % segment_id,
                                       headers=config.STRAVA_API_HEADER)
                    '''

                    #pprint.pprint(res.json())

                    try:
                        if res.status_code != 200 or "errors" in res.json():
                            pprint.pprint(res.json())
                            logger.info("Sleeping after Requesting Retry for Page: ", page_num)
                            time.sleep(60)
                            continue
                    except Exception as e:
                        logger.info(res)
                        logger.info("### Exception: ", e)
                        logger.info("Sleeping after Exception for Page: %s", page_num)
                        time.sleep(60*5)
                        continue

                    page_num += 1

                    for entry in res.json()['entries']:
                        #pprint.pprint(entry)
                        entry_num += 1
                        entry["segment_id"] = segment_id  # try using strava segment id instead of MongoDB id
                        entry["_segment_id"] = _id  # this is inserting as 'null' in MongoDB
                        leaderboard_batch.append(entry)
                    #if len(leaderboard_batch) == 0:
                    #    print "Empty leaderboard!"
                    try:
                        #Insert Efforts Batch into MongoDB
                        leaderboard_collection.initialize_unordered_bulk_op()
                        ids = leaderboard_collection.insert(leaderboard_batch)
                        #print(ids)
                        logger.info("[Segment:%s][Total:%d] Total Number of Leaderboard entries inserted into Mongo is %d",
                                                    segment_id, num_athletes, entry_num)
                    except pymongo.errors.DuplicateKeyError as dk:
                        logger.info("### Exception inserting leaderboard: %s", dk)
                    except Exception as e:
                        #any other exception
                        logger.info("### Exception inserting leaderboard: %s", e)
    print "Finished acquiring data: \n \
            Total Segments: " + str(segments_collection.count()) + "\n \
            Total Leaderboards: " + str(leaderboard_collection.count()) + "\n"

    print "Total acquisition time: " + str(datetime.datetime.now() - start)

if __name__ == '__main__':
    """Test"""
    get_zip()
