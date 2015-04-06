__author__ = 'ssatpati'


import pymongo
from pymongo import MongoClient
from ..util.config import Config
import pprint
from ..util import log

logger = log.getLogger(__name__)

# MongoDB Client & DB
cfg = Config()
client = MongoClient(cfg.get("mongo", "uri"))
db = client[cfg.get("mongo", "db_strava")]
segments_collection = db[cfg.get("mongo", "coll_segment")]
leaderboard_collection = db[cfg.get("mongo", "coll_leaderboards")]
zip_data_collection = db[cfg.get("mongo", "coll_zip")]
wban_collection = db[cfg.get("mongo", "coll_wban")]
weather_collection = db[cfg.get("mongo", "coll_weather")]

logger.info("Total # of Zip Codes: %d", zip_data_collection.count())

logger.info("Total # of Segments: %d", segments_collection.count())
logger.info("Total # of Leaderboards: %d", leaderboard_collection.count())

logger.info("Total # of WBAN: %d", wban_collection.count())
logger.info("Total # of Hourly Records: %d", weather_collection.count())

pprint.pprint(db.command("dbstats"))
