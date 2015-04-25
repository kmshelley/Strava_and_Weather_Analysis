__author__ = 'ssatpati'

import pymongo
from pymongo import MongoClient
from ..util import log
from ..util.config import Config
from tabulate import tabulate

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]


segment_fields = ("id", "name", "city", "activity_type", "distance", "athlete_count", "effort_count", "star_count")
segment_header = ["ID", "Name", "City", "Activity Type", "Distance", "Athlete Count", "Effort Count", "Star Count"]

leaderboard_fields = ()
leaderboard_header = []


def query_coll(cursor, fields, header, msg):
    """Generic Method for Displaying a Cursor"""
    logger.info(msg)
    table = []
    for s in cursor:
        l = []
        for f in fields:
            l.append(s[f])
        table.append(l)
    logger.info("\n" + tabulate(table, header, tablefmt="grid") + "\n\n")


def query_segments(cursor, msg):
    query_coll(cursor, segment_fields, segment_header, msg)


query_segments(segments_collection.find().sort("athlete_count", -1).limit(10),
               "@@@ Top 10 segments by Athlete Count @@@")

query_segments(segments_collection.find().sort("effort_count", -1).limit(10),
               "@@@ Top 10 segments by Effort Count @@@")

query_segments(segments_collection.find().sort("star_count", -1).limit(10),
               "@@@ Top 10 segments by Star Count @@@")

query_segments(segments_collection.find({"activity_type": "Run"}).sort("athlete_count", -1).limit(10),
               "@@@ Top 10 'Run' segments @@@")

query_segments(segments_collection.find({"city": "San Francisco"}).sort("athlete_count", -1).limit(10),
               "@@@ Top 10 segments in San Francisco @@@")




