# Sparkify Data Warehouse

## Summary

This is the Sparkify Data Warehouse built on Casssandra. It's main goal is to support the Analytics team in analyzing user song listening preferences. It contains 3 tables that support the Analytics team's queries

## DataSet Description  

### Song Dataset  
The dataset is available in csv files extracted from the Sparkify music app logs. Each file contains metadata about user listening sessions such as artist name, album, user name, session id, song and duration.  All the files are consolidated into a single csv file before being loaded into Cassandra tables.

### Tables supporting queries  
There are 3 tables called user_session, user_songs and user_search. Each one of them supports a unique query as required by the analytics team.  

#### user_session table  
This table is used to get the artist name, song tile and song length for a particular session id and the item number in that session.    

#### user_songs table  
This table is used to fetch the artist name, song and user for a particular user id and session id.  

#### user_search table  
This table is used to search for users that listened to particular song  


### Runtime instructions  

Tables in Cassarndra tables can be created and loaded with data using the etl_cass.py script. To run the script, run the following command:  

python etl_cass.py  


### Files included in the project  

README.md - this markup file  

etl_cass.py - python script that connects to the Cassandra database, creates the tables to support Sparkify anyalytics queries and loads them with data provided.  

