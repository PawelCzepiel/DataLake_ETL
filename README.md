# 1. PURPOSE

Sparkify data resides in AWS S3 in 2 directories containing:
- logs on user activity on the app
- metadata on the songs in the app

In order to improve data analytics, it is advised to move data into
data lake together with ETL pipeline extracting data from input S3 bucket
and eventually creating a star schema architecture on output S3 bucket, allowing for easy quering.

# 2. MANUAL

In order to perform inteded actions, as describe in Purpose section, etl.py python files 
needs to be executed.
To do so:
1. Open command line terminal
2. Navigate the terminal to the directory where the script is located using the cd command.
3. Type "python etl.py" in the terminal to execute the script and make sure no errors were returned.

# 3. STRUCTURE

The repository consists of the following:

## 3.1 etl.py

ETL script extracts data from JSON files located on input S3 bucket, loads it into staging tables 
followed by moving the data to final star schema and saving it on output S3 bucket.

## 3.4 dl.cfg

Stores IAM role info. 

## 3.5 README.md

Here.

# 4. SCHEMA & PIPELINE

Sparkify Redshift database Schema is of a Star type, providing best results for OLAP.
This architecture shall allow for fast aggregations and simple queries (also those not yet foreseen).

## 4.1 Staging Dataframes
Before the data lands in the final tables, it is initially loaded inside the staging dataframes:
- logs dataframe
            artist, auth, firstName, gender, itemInSession, lastName, length, level, location,
            method, page, registration, ressionId, song, status, ts, userAgent, userId

-songs dataframe
            num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, 
            song_id, title, duration, year

## 4.2 Final Schema

The final schema consists of:
- centre Fact table:
    songplays - records in event data associated with song plays i.e. records with page NextSong
                songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- 4 dimension tables:
    users - users in the app
                user_id, first_name, last_name, gender, level
    songs - songs in music database
                song_id, title, artist_id, year, duration
    artists - artists in music database
                  artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
                  start_time, hour, day, week, month, year, weekday

# 5. EXAMPLE QUERIES

# 5.1 Getting song title and count of its plays

SELECT title, COUNT(*) FROM songplays sp 
JOIN songs s ON sp.song_id = s.song_id
GROUP BY title
