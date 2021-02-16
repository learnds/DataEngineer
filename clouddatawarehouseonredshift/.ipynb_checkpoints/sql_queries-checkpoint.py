import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists \"user\""
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events(
    artist varchar(1000),
    auth varchar(2000),
    firstName varchar(100),
    gender varchar(20),
    itemInSession integer,
    lastName varchar(100),
    length float,
    level varchar(20),
    location varchar(100),
    method varchar(20),
    page varchar(20),
    registration varchar(20),
    sessionId integer,
    song varchar(1000),
    status integer,
    ts bigint,
    userAgent varchar(1000),
    userId integer
    )
""")

staging_songs_table_create = ("""
create table staging_songs(
num_songs integer,
artist_id varchar(100),
artist_latitude varchar(100) null,
artist_longitude varchar(100) null,
artist_location varchar(1000) null,
artist_name varchar(1000),
song_id varchar(100),
title varchar(1000),
duration float,
year integer null)
""")

songplay_table_create = ("""
create table songplays (
songplay_id bigint IDENTITY(0,1) not null,
start_time timestamp not null sortkey, 
user_id integer not null, 
level varchar(20), 
song_id varchar(100) not null distkey,
artist_id varchar(100) not null, 
session_id integer,
location varchar(1000), 
user_agent varchar(1000)
)
""")

user_table_create = ("""
create table "user"(
user_id integer not null sortkey, 
first_name varchar(100) not null, 
last_name varchar(100) not null, 
gender varchar(2), 
level varchar(20)
)
diststyle all
""")

song_table_create = ("""
create table song(
song_id varchar(100) not null sortkey distkey, 
title varchar(1000), 
artist_id varchar(100) not null, 
year integer not null, 
duration integer not null
)
""")

artist_table_create = ("""
create table artist(
artist_id  varchar(100) not null sortkey, 
name varchar(1000) not null, 
location varchar(1000) null, 
latitude varchar(100) null, 
longitude varchar(100) null
)
diststyle all
""")

time_table_create = ("""
create table time (
start_time timestamp not null sortkey, 
hour int  not null, 
day int  not null, 
week  int not null,
month int  not null, 
year int  not null,
weekday int  not null
)
diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data/' 
credentials 'aws_iam_role={}'
 region 'us-west-2' 
 json 's3://udacity-dend/log_json_path.json';
""")

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data/' 
credentials 'aws_iam_role={}'
 region 'us-west-2' 
 json 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
select d.start_time,b.userid,b.level,
c.song_id,a.artist_id,
b.sessionid, b.location, b.useragent
from artist a, staging_events b, song c,time d
where a.name=b.artist
and c.title=b.song
and d.start_time=date_add('ms',ts,'1970-01-01')
and b.page='NextSong'
""")

user_table_insert = ("""
insert into "user"(first_name,gender,last_name,level,user_id)
select firstName,gender,lastName,level,userId
from (
select  firstName,gender,lastName,level,userId,
  row_number() over(partition by userId order by level,firstName) as rownum
from staging_events
where userId is not null
 )
where rownum=1
""")

song_table_insert = ("""
insert into song (artist_id, duration,song_id,title,year)
select distinct artist_id,duration, song_id,title,year from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into artist (artist_id,name,location,latitude,longitude)
select artist_id,artist_name,artist_location,artist_latitude,artist_longitude from (
select  artist_id,artist_name,artist_location,artist_latitude,artist_longitude,
row_number() over (partition by artist_id order by year,artist_location) as  rownum
from staging_songs a
where artist_id is not null
)
where rownum=1
""")

time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
select 
cast(timecol as timestamp) as start_time,
extract(hour from timecol) as hour,
extract(day from timecol) as day,
extract(week from timecol) as week,
extract(month from timecol) as month,
extract(year from timecol) as year,
extract(dow from timecol) as weekday
from(
SELECT date_add('ms',timestamp_col,'1970-01-01') as timecol
from ( select distinct ts as timestamp_col from staging_events)
  )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, user_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
