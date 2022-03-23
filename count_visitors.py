import time
import sys
import sqlite3
from datetime import datetime

DB_NAME = "db.sqlite"

def get_lines(time_obj): '''Connect to the database.
                            Query any rows that have been added after a certain timestamp.
                            Fetch all the rows.'''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT remote_addr,time_local FROM logs WHERE created > ?", [time_obj])
    resp = cur.fetchall()
    return resp

'''We then need a way to extract the ip and time from each row we queried.

1. Initialize two empty lists.
2. Pull out the time and ip from the query response and add them to the lists.'''

def get_time_and_ip(lines):
    ips = []
    times = []
    for line in lines:
        ips.append(line[0])
        times.append(parse_time(line[1]))
    return ips, times

def parse_time(time_str): #code for parsing time from a string into a datetime object
    try:
        time_obj = datetime.strptime(time_str, '[%d/%b/%Y:%H:%M:%S %z]')
    except Exception:
        time_obj = ""
    return time_obj

if __name__ == "__main__":
    unique_ips = {}
    counts = {}
    start_time = datetime(year=2017, month=3, day=9)
    while True:
        lines = get_lines(start_time) #Get the rows from the database based on a given start time to query from (we get any rows that were created after the given time).
        ips, times = get_time_and_ip(lines) #Extract the ips and datetime objects from the rows.
        if len(times) > 0: #If we got any lines, assign start time to be the latest time we got a row. This prevents us from querying the same row multiple times.
            start_time = times[-1]
        for ip, time_obj in zip(ips, times):
            day = time_obj.strftime("%d-%m-%Y")
            if day not in unique_ips: #Create a key, day, for counting unique ips.
                unique_ips[day] = set()
            unique_ips[day].add(ip) #Add each ip to a set that will only contain unique ips for each day.
#counting
        for k, v in unique_ips.items(): #Assign the number of visitors per day to counts
            counts[k] = len(v)

        count_list = counts.items()
        count_list = sorted(count_list, key=lambda x: x[0])

        print("")
        print(datetime.now())
        for item in count_list:
            print("{}: {}".format(*item)) #Print out the visitor counts per day.

        time.sleep(5)
