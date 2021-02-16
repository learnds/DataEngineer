 # Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

def build_new_csv_file(new_file_name):
    '''
    This function creates a consolidated file based on the input file name from all the files in
    the event_data folder.
    Input parameters
        new_file_name: Name of the consolidated file to be created
    Returns: No return value
    '''
    # checking your current working directory
    print(os.getcwd())

    # Get your current folder and subfolder event data
    
    filepath = os.getcwd() + '/event_data'
    list_of_files = []

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
    
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*events.csv'))
        #print(file_path_list,sep=", ", end="\n")
        #Prepare a list of all files
        list_of_files.extend(file_path_list)
        
        
    print("Total number of files is ",len(list_of_files))

    full_data_rows_list = [] 
    
    # for every filepath in the file path list 
    for f in list_of_files:
        print("Reading file ",f)
    # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
        
            # extracting each data row one by one and append it        
            for line in csvreader:
            #print(line)
                full_data_rows_list.append(line) 
            
    print ("Total Number of records in list is ",len(full_data_rows_list))
   
    # creating a consolidated event data csv file called new_file_name csv that will be used to insert data into the \
    # Apache Cassandra tables
   
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    
    print("Writing to a consolidated file called "+new_file_name)
    with open(new_file_name, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    print ("Total numnber of records in file is ",len(list(open(new_file_name))))
    

def create_user_session(session1,file_name):
    '''
    This function creates the user_session table in cassandra and loads it with data from the input file

    Input parameters
        session1: session variable with connection to the Cassandra database
        file_name: Name of the consolidated file to load data from
    Returns: No return value
    '''
    ## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
    ## sessionId = 338, and itemInSession = 4
    query = "CREATE TABLE IF NOT EXISTS user_session"
    query = query + "(sessionid int, itemsinsession int, artist text, song text, songlength float, PRIMARY KEY(sessionid,itemsinsession) )"
    try:
        session1.execute(query);
    except Exception as e:
        print(e)
        
    # We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
    file = file_name
    print ("\nInserting records into user_sesion")
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
        ##  Assign the INSERT statements into the `query` variable
            query = "Insert into user_session (sessionid, itemsinsession,artist,song,songlength)"
            query = query + " values(%s, %s, %s, %s, %s)"
            ## Assign which column element should be assigned for each column in the INSERT statement.
            try:
                session1.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
            except Exception as e:
                print(e)
           # print ("Data values are "+ (line[8]), (line[3]), line[0], line[9], line[5])
        
    ## Add in the SELECT statement to verify the data was entered into the table
    print("Verifying user_session table")
    query = "select artist, song, songlength from user_session where sessionid=338 and itemsinsession=4"
    try:
        rows=session1.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row.artist,row.song,row.songlength)
        


def create_user_songs(session2,file_name):
    '''
    This function creates the user_songs table in cassandra and loads it with data from the input file

    Input parameters
        session2: session variable with connection to the Cassandra database
        file_name: Name of the consolidated file to load data from
    Returns: No return value
    '''
    ## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
    ## for userid = 10, sessionid = 182
    query = "create table if not exists user_songs"
    query = query + "(userid int, sessionid int,itemsinsession int, artist text, song text, user text, Primary key((userid,sessionid),itemsinsession) )"

    try:
        session2.execute(query);
    except Exception as e:
        print(e)
        
    file = file_name
    
    print("\n\nInserting into user_songs")

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the `query` variable
            query = "Insert into user_songs (userid, sessionid,itemsinsession,artist, song,user)"
            query = query + " values(%s, %s, %s, %s, %s,%s)"
            ## Assign which column element should be assigned for each column in the INSERT statement.
            try:
                session2.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1]+" "+line[4] ))
            except Exception as e:
                print(e)
    
    print("Verifying user_songs table")
    query = "select artist, song, user from user_songs where userid = 10 and sessionid = 182"
    try:
        rows=session2.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row.artist, row.song, row.user)
    
def create_user_search(session3,file_name):
    '''
    This function creates the user_search table in cassandra and loads it with data from the input file

    Input parameters
        session3: session variable with connection to the Cassandra database
        file_name: Name of the consolidated file to load data from
    Returns: No return value
    '''
    ## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    query = "create table if not exists user_search"
    query = query + "(song text, userid int, user text, primary key(song,userid))"

    try:
        session3.execute(query);
    except Exception as e:
        print(e)
        
    file = file_name
    print("\nInserting into user_search")

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## : Assign the INSERT statements into the `query` variable
            query = "Insert into user_search (song, userid, user)"
            query = query + " values(%s, %s, %s)"
            ## Assign which column element should be assigned for each column in the INSERT statement.
            try:
                session3.execute(query, (line[9], int(line[10]), line[1]+" "+line[4]))
            except Exception as e:
                print(e)
                
    print("Verifying user_search")
    query = "select user from user_search where song = 'All Hands Against His Own'"
    try:
        rows=session3.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row.user)

def drop_tables(session4):
    '''
    This function drop all the tables created in Cassandra database

    Input parameters
        session4: session variable with connection to the Cassandra database
    Returns: No return value
    '''
    ##  Drop the table before closing out the sessions
    query = "drop table user_session"
    try:
        rows=session4.execute(query)
    except Exception as e:
        print(e)
    
    query = "drop table user_songs"
    try:
        rows=session4.execute(query)
    except Exception as e:
        print(e)
    
    query = "drop table user_search"
    try:
        rows=session4.execute(query)
    except Exception as e:
        print(e)
        
def main(): 
    '''
    The main block - connects to the cassandra database and calls relevant functions 
                     to process cvs files and load them into the database
    
    '''

    new_csv_file ='new_event_datafile.csv'

    build_new_csv_file(new_csv_file)
    
    # This should make a connection to a Cassandra instance your local machine 
    # (127.0.0.1)

    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['127.0.0.1'])
        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
    except Exception as e:
        print(e)
        
        
     # Create a Keyspace 
    try:
        session.execute ("""Create keyspace if not exists events \
                      with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                    )
    except Excception as e:
        print(e)  
        
    # Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('events')
    except Exception as e:
        print(e)

    create_user_session(session,new_csv_file)
    
    create_user_songs(session,new_csv_file)
              
    create_user_search(session,new_csv_file)

    # Drop all tables
    drop_tables(session)
    
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
    