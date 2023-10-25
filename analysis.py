import configparser
import psycopg2
from tabulate import tabulate



"""data validations - SQL query to count all the records in each table"""
def count_rows(cur, conn):
    count_stage_events = ("""SELECT count(*) FROM stage_events;""")
    count_stage_songs = ("""SELECT count(*) FROM stage_songs;""")
    count_songplay = ("""SELECT count(*) FROM songplay;""")
    count_users = ("""SELECT count(*) FROM users;""")
    count_song = ("""SELECT count(*) FROM song;""")
    count_artist = ("""SELECT count(*) FROM artist;""")
    count_time = ("""SELECT count(*) FROM time;""")

    count_list = [count_stage_events, count_stage_songs, count_songplay, count_users, count_song, count_artist, count_time]
    tables = ['stage_events', 'stage_songs', 'songplay', 'users', 'song', 'artist', 'time']
    i = 0
    
    print("\nNumber of records:")
    
    for query in count_list:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(tables[i], ": ", str(row[0]), "records")
            i = i+1
        conn.commit()
        #print("\n")

"""sample question - SQL query select top 10 mosted played song"""
def most_played(cur, conn):
    cur.execute("""
        SELECT s.title as song, a.name as artist, COUNT(*) AS play_count
        FROM songplay sp
        LEFT JOIN song s
            ON s.song_id = sp.song_id
        JOIN artist a
            ON s.artist_id = a.artist_id
        WHERE s.song_id IS NOT NULL
        GROUP BY s.title, a.name
        ORDER BY play_count DESC
        LIMIT 10;
        """)
    conn.commit()
    
    rows = cur.fetchall()
    print("\nTop 10 most played song:\n")
    print(tabulate(rows, headers=['song', 'artist', 'play_count'], tablefmt='psql'))
    
"""sample question - SQL query select top 5 busiest hour in the day that songs are played"""
def busy_hour(cur, conn):
    cur.execute("""
        SELECT hour, COUNT(*) as usage_count
        FROM time
        GROUP BY hour
        ORDER BY usage_count DESC
        LIMIT 5;
        """)
    conn.commit()
    
    rows = cur.fetchall()
    print("\nTop 5 buisest hour in a day:\n")
    print(tabulate(rows, headers=['hour', 'usage_count'], tablefmt='psql'))

"""connection to Redshift cluster"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    count_rows(cur, conn)
    most_played(cur, conn)
    busy_hour(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()






