# Sparkify_DWH
## Sparkify ETL Pipeline using S3 -> Redshift

### Background

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. This project creates an ETL pipeline for Sparkify that combines different datasets stored in a multitude of files and stores them in a database for analysts to easily query the data to find insight. 

### Database Schema Design:

Below are the following tables analysts have access to:
- artists - information on artists such as their name, a unique artist id, their location and latitude and longitude data if available
- users - information on the user's first and last name, a unique id, their gender and whether they have a paid or free account
- time - information on when a user listened to a song such as the starttime and broken down into what hour, day, week, month, year and week of year
- songs - information on song titles, a unique song id, an artist id, the year the song was release and how long the song was for
- song_play - information on when a user played a song, what song, what artist, what user and what session and location user was on

This ETL pipeline uilized a star schema to optimize and simplify quieries which will often be aggregation quieries.  

### Use Cases:
1. Top played artist
```
select a.name, count(*) num_played
from song_play as sp
join artists  as a
	on sp.artist_id = a.artist_id 
group by 1
```

2. Most active users (Top 10)
```
select u.user_id, u.first_name, count(*) times_active
from song_play as sp
join users  as u
 	on sp.user_id = u.user_id 
group by u.user_id, u.first_name
order by 3 desc
limit 10
```


### Instructions to run:
- create: IAM Role, IAM User, Redshift Cluster
- add relevant information from creations above to dwh.cfg file
- run 'create_tables.py' which creates all of the tables you will need
- run 'etl.py' which inserts all of the data into your tables
- From there run various queries off of your Redshift database using AWS's query editor 

