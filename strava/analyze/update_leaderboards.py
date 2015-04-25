__author__ = 'ssatpati'

import pymongo
from pymongo import MongoClient
from pymongo import GEOSPHERE
from pymongo.errors import BulkWriteError
from bson.code import Code
from bson.son import SON
import pprint
from ..util import log
from ..util.config import Config
import sys
import time

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
wban_collection = db[cfg.get("mongo", "coll_wban")]


def update_leaderboards_wban():
    """Update Leaderboard with WBAN"""
    t1 = time.time()
    logger.info("Leaderboard: Creating Index on Segment ID")
    leaderboard_collection.ensure_index("segment_id")
    wban_collection.ensure_index([('loc', GEOSPHERE)])
    t2 = time.time()
    logger.info("Time taken in index creation: {0} seconds".format(t2 - t1))


    cnt = 0
    d_seg = {}
    #c_seg = segments_collection.find({"id": 1013233}, no_cursor_timeout=True)
    c_seg = segments_collection.find()

    for c in c_seg:
        cnt += 1
        d_seg[c["id"]] = (c["name"], c["start_latlng"])

    pprint.pprint(d_seg)
    logger.info("Total # of segments: {0}".format(cnt))


    # Reset
    cnt = 0

    for k in d_seg:
        try:
            cnt += 1
            logger.info("[{0}] Processing Segment: {1} - {2}".format(cnt, k,
                                                                     d_seg[k][0].encode("utf-8") if d_seg[k][0] else "None"))

            # Swap lat/long to long/lat to make it work with mongo geo
            coordinates = d_seg[k][1]
            coordinates[0], coordinates[1] = coordinates[1], coordinates[0]

            # Get the WBAN Information for Segment

            #c_wban = wban_collection.find({"coordinates": {"$near": coordinates}}).limit(1)
            c_wban = db.command(SON([('geoNear', 'WBAN'),
                                    ('near', coordinates),
                                    ('spherical','true'),
                                    ('limit', 1)]))['results'][0]['obj']
            #pprint.pprint(c_wban)
            logger.info("WBAN: {0}|{1}|{2}".format(c_wban["WBAN_ID"], c_wban["STATE_PROVINCE"], c_wban["COUNTRY"]))

            # Update Leaderboard with WBAN ID

            #logger.info(leaderboard_collection.find({"segment_id": c1["id"]}).count())
            msg = leaderboard_collection.update({"segment_id": k}, {'$set': {"WBAN": c_wban["WBAN_ID"]}},
                                          upsert=False,
                                          multi=True,
                                          safe=True)

            logger.info(msg)
        except Exception as e:
            logger.exception("### Exception updating leaderboard: {0}".format(e))
            continue

if __name__ == '__main__':
    update_leaderboards_wban()
