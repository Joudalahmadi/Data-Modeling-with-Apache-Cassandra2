#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


import cassandra
import pandas as pd
import csv
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


import os

# Specify the directory where your event data files are located
directory = 'event_data'

# Create an empty list to store the file paths
file_paths = []

# Iterate over the files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        file_paths.append(file_path)

# Print the list of file paths
for file_path in file_paths:
    print(file_path)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[2]:


import csv
import os

# Step 1: Create a list to store the data from the original CSV files
data = []

# Step 2: Get the filepaths for all the event data files
folder_path = 'event_data'
file_path_list = []
for root, dirs, files in os.walk(folder_path):
    file_path_list += [os.path.join(root, file) for file in files if file.endswith('.csv')]

# Step 3: Read the data from each file and append it to the data list
for file_path in file_path_list:
    with open(file_path, 'r', encoding='utf8', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header row
        for row in csvreader:
            data.append(row)

# Step 4: Create the event_data_new.csv file and write the data to it
output_file = 'event_data_new.csv'
header = ['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'userId']
with open(output_file, 'w', encoding='utf8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(header)
    for row in data:
        csvwriter.writerow(row)


# In[3]:


import csv
import os

# Create a list to store the data from the original CSV files
data = []

# Get the filepaths for all the event data files
folder_path = 'event_data'
file_path_list = []
for root, dirs, files in os.walk(folder_path):
    file_path_list += [os.path.join(root, file) for file in files if file.endswith('.csv')]

# Read the data from each file and append it to the data list
for file_path in file_path_list:
    with open(file_path, 'r', encoding='utf8', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header row
        for row in csvreader:
            data.append(row)

# Create the event_data_new.csv file and write the data to it
output_file = 'event_data_new.csv'
header = ['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'userId']
with open(output_file, 'w', encoding='utf8', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(header)
    for row in data:
        csvwriter.writerow(row)


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[4]:


from cassandra.cluster import Cluster

# Create a cluster object and connect to the cluster
cluster = Cluster(['127.0.0.1'])  # Replace '127.0.0.1' with the IP address of your Cassandra cluster
session = cluster.connect()

# Perform operations on the cluster
# ...

# Close the connection to the cluster
session.shutdown()
cluster.shutdown()


# from cassandra.cluster import Cluster
# 
# # Create a cluster object and connect to the cluster
# cluster = Cluster(['127.0.0.1'])  # Replace '127.0.0.1' with the IP address of your Cassandra cluster
# session = cluster.connect()
# 
# # Perform operations on the cluster
# # ...
# 
# # Close the connection to the cluster
# session.shutdown()
# cluster.shutdown()

# In[5]:




# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create a keyspace
keyspace_name = 'my_keyspace'
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")

# Set the keyspace
session.set_keyspace(keyspace_name)

# Perform operations within the keyspace
# ...

# Close the connection
session.shutdown()
cluster.shutdown()


# #### Set Keyspace

# In[6]:


from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Set the keyspace
keyspace_name = 'my_keyspace'
session.execute("USE " + keyspace_name + ";")

# Perform operations within the keyspace
# ...

# Close the connection
session.shutdown()
cluster.shutdown()


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[7]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create a keyspace
keyspace_name = 'my_keyspace'
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")

# Set the keyspace
session.set_keyspace(keyspace_name)

# Create a table
table_name = 'music_history'
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sessionId INT,
        itemInSession INT,
        artist TEXT,
        songTitle TEXT,
        songLength FLOAT,
        PRIMARY KEY (sessionId, itemInSession)
    )
"""
session.execute(create_table_query)

# Insert data into the table
insert_query = f"""
    INSERT INTO {table_name} (sessionId, itemInSession, artist, songTitle, songLength)
    VALUES (338, 4, 'Artist Name', 'Song Title', 123.45)
"""
session.execute(insert_query)

# Query the table to answer the question
select_query = f"""
    SELECT artist, songTitle, songLength
    FROM {table_name}
    WHERE sessionId = 338 AND itemInSession = 4
"""
result = session.execute(select_query)

# Print the result
for row in result:
    print(row.artist, row.songtitle, row.songlength)

# Close the connection
session.shutdown()
cluster.shutdown()


# In[28]:


file = 'event_data_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    
    for row in csvreader:
        print(row)


# #### Do a SELECT to verify that the data have been inserted into each table

# In[29]:


import sqlite3
import csv

# Connect to the SQLite database
conn = sqlite3.connect('my_database.db')

# Create a cursor object
cur = conn.cursor()

# Create the 'event' table
cur.execute('''
CREATE TABLE IF NOT EXISTS event (
    id INTEGER PRIMARY KEY,
    event_name TEXT,
    event_location TEXT,
    event_date TEXT
)
''')

# Create the 'guest' table
cur.execute('''
CREATE TABLE IF NOT EXISTS guest (
    id INTEGER PRIMARY KEY,
    guest_name TEXT,
    guest_email TEXT,
    event_id INTEGER,
    FOREIGN KEY (event_id) REFERENCES event (id)
)
''')

# Commit the transaction
conn.commit()

# Open the original CSV file
with open('event_data_new.csv', 'r', encoding='utf8', newline='') as original_file:
    csvreader = csv.reader(original_file)
    next(csvreader) # Skip the header row

    # Iterate through each row in the original CSV file
    for row in csvreader:
        # Split the row into a list of values
        values = row[0].split(',')

        # Print the row values
        print("Row values: ", values)

        # Check if the values list has enough elements
        if len(values) >= 5:
            # Insert the event data into the 'event' table
            cur.execute('''
            INSERT INTO event (event_name, event_location, event_date)
            VALUES (?, ?, ?)
            ''', (values[0], values[1], values[2]))

            # Get the ID of the last inserted event
            event_id = cur.lastrowid

            # Insert the guest data into the 'guest' table
            cur.execute('''
            INSERT INTO guest (guest_name, guest_email, event_id)
            VALUES (?, ?, ?)
            ''', (values[3], values[4], event_id))

            # Commit the transaction
            conn.commit()
        else:
            print("Skipping row due to insufficient number of elements.")

# Now, you can use the SELECT statement to verify the data insertion
cur.execute('SELECT * FROM event')
event_rows = cur.fetchall()

for row in event_rows:
    print(row)

cur.execute('SELECT * FROM guest')
guest_rows = cur.fetchall()

for row in guest_rows:
    print(row)

# Close the connection to the SQLite database
conn.close()


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[8]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create a keyspace
keyspace_name = 'my_keyspace'
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")

# Set the keyspace
session.set_keyspace(keyspace_name)

# Create a table
table_name = 'music_history'
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sessionId INT,
        itemInSession INT,
        artist TEXT,
        songTitle TEXT,
        songLength FLOAT,
        PRIMARY KEY (sessionId, itemInSession)
    )
"""
session.execute(create_table_query)

# Insert data into the table
insert_query = f"""
    INSERT INTO {table_name} (sessionId, itemInSession, artist, songTitle, songLength)
    VALUES (338, 4, 'Artist Name', 'Song Title', 123.45)
"""
session.execute(insert_query)

# Query the table to answer the question
select_query = f"""
    SELECT artist, songTitle, songLength
    FROM {table_name}
    WHERE sessionId = 338 AND itemInSession = 4
"""
result = session.execute(select_query)

# Print the result
for row in result:
    print(row.artist, row.songtitle, row.songlength)

# Close the connection
session.shutdown()
cluster.shutdown()


                    


# In[45]:


# Establish a connection to Cassandra
cluster = Cluster(['127.0.0.1']) # replace 'localhost' with the IP of your Cassandra node
session = cluster.connect() # Connect to your keyspace
session.set_keyspace('my_keyspace') # replace 'my_keyspace' with the name of your keyspace

# Create the songs table if it doesn't exist
query = """
    CREATE TABLE IF NOT EXISTS songs (
        song text,
        FirstName text,
        LastName text,
        PRIMARY KEY (song, FirstName, LastName)
    )
"""
session.execute(query)

# Perform the SELECT query
query = "SELECT FirstName, LastName FROM songs WHERE song = 'All Hands Against His Own'"
rows = session.execute(query)

# Print the retrieved rows
for row in rows:
    print(row.FirstName, row.LastName)


# In[32]:


### TO-DO: Query 1: Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
from cassandra.cluster import Cluster

# Establish a connection to Cassandra
cluster = Cluster(['127.0.0.1'])  # replace 'localhost' with the IP of your Cassandra node
session = cluster.connect()  # Connect to your keyspace
session.set_keyspace('my_keyspace')  # replace 'my_keyspace' with the name of your keyspace

# Create the music_app_history table if it doesn't exist
query = """
    CREATE TABLE IF NOT EXISTS music_app_history (
        sessionId int,
        itemInSession int,
        artist text,
        song text,
        length float,
        PRIMARY KEY (sessionId, itemInSession)
    )
"""
session.execute(query)

# Perform the SELECT query
query = "SELECT artist, song, length FROM music_app_history WHERE sessionId = 338 AND itemInSession = 4"
rows = session.execute(query)

# Print the retrieved rows
for row in rows:
    print(row.artist, row.song, row.length)


# In[ ]:





# ### Drop the tables before closing out the sessions

# In[26]:


## TO-DO: Drop the table before closing out the sessions
def main():
    # Create the cluster and session
    cluster, session = create_cluster()
    
    # Process the CSV file
    process_csv_file(session, 'event_data.csv')
    
    # Drop the tables
    session.execute("DROP TABLE IF EXISTS session_library")
    session.execute("DROP TABLE IF EXISTS user_session_library")
    session.execute("DROP TABLE IF EXISTS song_library")
    
    # Close the cluster and session
    close_cluster(cluster, session)


# In[24]:


from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def connect_to_cassandra():
    # Create the cluster and session
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create the keyspace if it doesn't exist
    session.execute("CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")

    # Set the current keyspace
    session.set_keyspace('my_keyspace')

    return session

def create_songs_table(session):
    # Create the songs table
    session.execute("CREATE TABLE IF NOT EXISTS songs ("
                    "id UUID PRIMARY KEY,"
                    "title text,"
                    "artist text,"
                    "album text,"
                    "year int,"
                    "duration float"
                    ")"
                    "WITH compaction = {'class': 'SizeTieredCompactionStrategy'}")

def insert_song(session, song):
    # Insert a song into the songs table
    session.execute("INSERT INTO songs (id, title, artist, album, year, duration) VALUES (%s, %s, %s, %s, %s, %s)",
                    (song['id'], song['title'], song['artist'], song['album'], song['year'], song['duration']))

def select_song(session, song_id):
    # Select a song from the songs table
    rows = session.execute("SELECT * FROM songs WHERE id = %s", (song_id,))
    return rows[0]

def main():
    # Create the cluster and session
    session = connect_to_cassandra()

    # Create the songs table
    create_songs_table(session)

    # Insert a song into the songs table
    song = {'id': '56d2454e-63d7-463e-9697-9d5e7c286d7e',
            'title': 'Ice Cream',
            'artist': 'Cena',
            'album': 'You Can Do Anything',
            'year': 2018,
            'duration': 195.6}
    insert_song(session, song)

    # Select a song from the songs table
    result = select_song(session, song['id'])
    print("Selected song: ", result)

# Execute the script


# ### Close the session and cluster connection¶

# In[25]:


session.shutdown() 
cluster.shutdown() 
from cassandra.cluster import Cluster

# Establish a connection to Cassandra
cluster = Cluster(['127.0.0.1'])  # replace 'localhost' with the IP of your Cassandra node
session = cluster.connect()  # Connect to your keyspace
session.set_keyspace('my_keyspace')  # replace 'my_keyspace' with the name of your keyspace

# Perform your operations here...

# Close the session and cluster connection
session.shutdown()
cluster.shutdown()


# In[ ]:





# In[37]:





# In[ ]:




