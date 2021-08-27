#!/usr/bin/python
import psycopg2
from psycopg2 import Error
from config import config


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            print('Database connection is open.')
    return conn

def loadData():
    # create cursor to fetch all data
    conn = connect()
    cur = conn.cursor()

    # execute select statement to get all data from table booster_ads_app
    ad_query = """ SELECT id, owner_ad_id, google_campaign_id, facebook_campaign_id, campaign_time, is_active, expiry_date FROM booster_ads_app WHERE is_active IS true """
    cur.execute(ad_query)
    fetched_data = cur.fetchall()
    cur.close()
    return fetched_data

def compareId(all_campaign):
    conn = connect()
    cur = conn.cursor()
    onair_query = """ SELECT ad_id FROM onair where ad_id IS NOT null """
    cur.execute(onair_query)
    onair_data = cur.fetchall()
    cur.close()
    matched_id = []

    for all_campaign_data in all_campaign:
        for onair_data_tuple in onair_data:
            if all_campaign_data[1] == onair_data_tuple[0]:
                matched_id.append(onair_data_tuple)
            else:
                continue
    
    return matched_id

if __name__ == '__main__':
    all_campaign = loadData()
    match = compareId(all_campaign)
    