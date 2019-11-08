import os
import glob
import psycopg2
import pandas as pd
import math
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    print("Processing File: {}".format(filepath))
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

    
def data_fetch_first(cur):
    ret_list=[]
    row=cur.fetchone()
    while row:
        ret_list.append(row[0])
        row=cur.fetchone()
    return ret_list    
        
def process_log_file(cur, filepath):
    print("Processing File: {}".format(filepath))
    # log file correction
    cmd="sed -i 's/\\\\\\\"//g' "+filepath
    os.system(cmd)

    # open log file
    cur.execute(drop_log_file_table)
    cur.execute(create_log_file_temp_table)
    cur.execute(copy_data_from_json,[filepath])

    # convert timestamp column to datetime & insert time data records
    cur.execute(ts_fetch)
    for ts in data_fetch_first(cur):
        t = datetime.datetime.fromtimestamp(float(ts)/1000.0)
        time_data = [ts,t.hour,t.day,t.isocalendar()[1],t.month,t.year,t.weekday()]
        cur.execute(time_table_insert,time_data)

    # insert user records in bulk
    cur.execute(distinct_level_fetch)
    for level_from_log in data_fetch_first(cur):
        cur.execute(user_table_insert_from_json,[level_from_log])

    # insert songplay records in bulk
    cur.execute(songplay_table_insert_from_json)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    cur.execute("COMMIT;")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()