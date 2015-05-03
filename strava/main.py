from acquire import get_zip_codes
from acquire import weather_acquire
from acquire import strava_data_acquire_store
from store import data_backup_restore
__author__ = 'ssatpati'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''
    get_zip_codes.collect_zip_code_data() #for collecting zip code data
    weather_acquire.get_weather() #for collecting weather data
    strava_data_acquire_store.fetch_store_segment_and_leaderboards() #for collecting strava data
    data_backup_restore.run_full_backup() #for S3 backup
    data_backup_restore.run_full_restore() #for S3 restore
