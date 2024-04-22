#Libraries to import
import googleapiclient.discovery
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import iso8601
import isodate
import re
import streamlit as st


#Access Youtube API
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyDIfyenDb9pW6wOL2-GIiObdpyXnEPCyGA"
youtube = googleapiclient.discovery.build(
        api_service_name, api_version,developerKey=api_key)

#Streamlit code for Tittle and Captions


st.set_page_config(layout='wide')
st.title(":red[Youtube Data Harvesting and Warehousing]")
channel_id = st.text_input("Enter Your Channel ID")
with st.sidebar:
     st.header(":Violet[Skills Take Away]")
     st.write(":green[Python Scripting]")
     st.write(":green[Data Collection]")
     st.write(":green[Streamlit]")
     st.write(":green[API]")
     st.write(":green[Data Management using SQL]")


#CHannel Data collection


def channel_data(channel_id):
    try:
        try:
            request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
                )
            response = request.execute()
            if 'items' not in response:
                        st.error(f"Invalid channel id: {channel_id}")
                        st.error("Enter the correct 11-digit **channel_id**")
                        return None
        except HttpError as e:
                st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes', icon='🚨')
                st.error('An error occurred: %s' % e)
                return None
    except:
                st.error('You have exceeded your YouTube API quota. Please try again tomorrow.')


    data = { 
                    "channel_id":channel_id,
                    "channel_name":response["items"][0]["snippet"]["title"],
                    "channel_dis":response["items"][0]["snippet"]["description"],
                    "channel_playlist":response["items"][0]["contentDetails"]['relatedPlaylists']['uploads'],
                    "channel_vidc":response["items"][0]['statistics']['videoCount'],
                    "channel_sc":response["items"][0]['statistics']['subscriberCount'],
                    "channel_vc":response["items"][0]['statistics']['viewCount']
    }
    return data

# Playlist Data collection

def playlist_data(channel_id):  
    video_list=[]
    response = youtube.channels().list(id=channel_id,
                                        part="contentDetails").execute()
                                
        


    playlist_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    next_page=None
    while True:
        videolists= youtube.playlistItems().list(
                                        
                                        part = 'snippet',
                                        playlistId = playlist_id,
                                        maxResults=50,
                                        pageToken=next_page).execute()
        for i in range(len(videolists['items'])):
        
            video_list.append(videolists['items'][i]['snippet']['resourceId']['videoId'])
            next_page=videolists.get('nextPageToken')
        if next_page is None:
            break


    return video_list


# Convert duration in video data collection

def iso8601_to_seconds(duration):

    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    else:
        return None
    
# Video data collection

def video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
        )
        response = request.execute()
        for item in response['items']:
            
            duration = iso8601_to_seconds(item['contentDetails'].get('duration'))
            video_data1={                         
                            "channel_id":channel_id,
                            "video_id":item['id'],
                            "video_tittle":item['snippet']['title'],
                            "video_descrip":item['snippet'].get('description'),
                            #"video_tag":item['snippet'].get('tags'),
                            "published_at":item['snippet']['publishedAt'],
                            "view_count":item['statistics'].get('viewCount'),
                            "Like_count":item['statistics'].get('likeCount'),
                            "Favourite_count":item['statistics']['favoriteCount'],
                            "Comment_count":item['statistics'].get('commentCount'),
                            "Duration":duration,
                            "Thumb_Nails":item['snippet']['thumbnails']['default']['url'],
                            "caption_status":response['items'][0]['contentDetails']['caption']
                            
                        }
            video_data.append(video_data1)
    return video_data


# Comment data collection
def comment_info_data(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId = video_id,
                maxResults=100
            )

            
            response = request.execute()

            
            comments=response['items']
            for comment in comments:
                comment_information = {"video_id":video_id,"channel_id":channel_id,
                            "Comment_Id": comment['snippet']['topLevelComment']['id'],
                            "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                            
                            }
                comment_data.append(comment_information)
            
        
    except:
        pass

    return comment_data

# using Streamlit get button to run a code call all function after click a button
get_data=st.button(':blue[Get Data and transfer to DF]')
if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
if get_data or st.session_state.Get_state:
        st.session_state.Get_state = True

# Function call           
        channel_detail=channel_data(channel_id)
        playlist_details=playlist_data(channel_id)
        video_info_data=video_info(playlist_details)
        comment_details=comment_info_data(playlist_details)

#Convert to Dataframe
        channel_df = pd.DataFrame([channel_detail])
        video_df=pd.DataFrame(video_info_data)
        comment_df=pd.DataFrame(comment_details)
        st.write(":green[Your database is ready]",":100:")
       
# Use Migrate button to tansfer dataframes to local Database
migrate_data = st.button(':violet[Migrate Data to MySQL]')
        
if migrate_data:
      #Database connection
      mydb = mysql.connector.connect(host="localhost",user="root",password="")
      mycursor = mydb.cursor(buffered=True)
      db_connection_string = f'mysql+mysqlconnector://root@localhost/youtube_final'
      db_engine = create_engine(db_connection_string)

    #Channel Table creation
      mycursor.execute('create database if not exists youtube_final')
      mycursor.execute('USE youtube_final')
      mycursor.execute('''create table if not exists channels(channel_id VARCHAR(50) PRIMARY KEY,channel_name VARCHAR(50),
                    channel_dis VARCHAR(100),channel_playlist TEXT,channel_vidc INT(10),channel_sc INT(10),
                    channel_vc INT(50))''')
      mydb.commit()
      channel_df.to_sql('channels', con=db_engine, if_exists='append', index=False)

    # Video Table creation
      mycursor.execute('create database if not exists youtube_final')
      mycursor.execute('USE youtube_final')
      mycursor.execute('''create table if not exists video(channel_id VARCHAR (50),FOREIGN KEY(channel_id) references channels(channel_id),video_id VARCHAR(50),video_tittle TEXT,video_descrip TEXT,
                    published_at TEXT,view_count INT(50),like_count INT(10),Favourite_count INT(10),Comment_count INT(20),
                    Duration INT(10),Thumb_Nails TEXT,
                    caption_status TEXT)''')
      mydb.commit()
      video_df.to_sql('video', con=db_engine, if_exists='append', index=False)


    # Comment Table creation
      mycursor.execute('create database if not exists youtube_final')
      mycursor.execute('USE youtube_final')
      mycursor.execute('''create table if not exists comment(video_id VARCHAR(50),channel_id VARCHAR (50),FOREIGN KEY (channel_id) references channels(channel_id),
                    Comment_Id VARCHAR(50),Comment_Text TEXT,Comment_Author TEXT,Comment_PublishedAt TEXT)''')
      mydb.commit()
      comment_df.to_sql('comment', con=db_engine, if_exists='append', index=False)
      
      st.success('Data migrated successfully to MySQL!')

# Database connection using SQLAlchemy
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
db_connection_string = f'mysql+mysqlconnector://root@localhost/youtube_final'
db_engine = create_engine(db_connection_string)

# show channel name,video,id,commentid in side bar(optional)
data_migrate=st.sidebar.selectbox("Data Migrate from database",("select","channel","video","comment"))

if data_migrate=="channel":
    mycursor.execute("SELECT channel_name from youtube_final.channels")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

elif data_migrate=="video":
      mycursor.execute("SELECT video_id from youtube_final.video")

      df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
      st.write(df)

elif data_migrate=="comment":
      mycursor.execute("SELECT Comment_Id from youtube_final.comment")

      df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
      st.write(df)
     
 

# 10 Queries - write using selectbox or dropdown.

query_select=st.selectbox("Select your Query",("Select your Query",
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
"10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

mycursor.execute('USE youtube_final')

#Query 1
if query_select=="1.What are the names of all the videos and their corresponding channels?":
    mycursor.execute("SELECT video.video_tittle, channels.channel_name \
    FROM youtube_final.video \
    INNER JOIN channels ON video.channel_id = channels.channel_id")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[name of videos and corresponding channels]")
    st.write(df)

#Query 2
elif query_select=="2.Which channels have the most number of videos, and how many videos do they have?":
    mycursor.execute('SELECT channel_name,max(channel_vidc) as max_videocount FROM youtube_final.channels LIMIT 1')

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Maximum video count]")
    st.write(df)

#Query 3
elif query_select== "3.What are the top 10 most viewed videos and their respective channels?":
    mycursor.execute("SELECT channels.channel_name, video.view_count \
                 FROM youtube_final.video \
                 JOIN channels ON video.channel_id = channels.channel_id \
                 ORDER BY video.view_count DESC LIMIT 10")
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Top 10 Most viewed video and their channel name]")
    st.write(df)

#Query 4
elif query_select=="4.How many comments were made on each video, and what are their corresponding video names?":
    mycursor.execute("SELECT video_tittle,Comment_count FROM youtube_final.video ORDER BY Comment_count DESC")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[comments on each video and cor video name]")
    st.write(df)

#Query 5
elif query_select=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, video.like_count \
    FROM youtube_final.video \
    JOIN channels ON video.channel_id = channels.channel_id \
    WHERE video.like_count = (SELECT MAX(like_count) FROM video)")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Highest Likes and their channel name]")
    st.write(df)

#Query 6
elif query_select=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    mycursor.execute("SELECT video_tittle,like_count FROM youtube_final.video GROUP BY video_tittle ORDER BY like_count DESC")
    st.write(":green[Total number of likes and their channel names]")
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

#Query 7
elif query_select== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    mycursor.execute('SELECT channel_name,channel_vc FROM youtube_final.channels ORDER by channel_vc DESC')
    
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[views for each channel]")
    st.write(df)

#Query 8
elif query_select=="8.What are the names of all the channels that have published videos in the year 2022?":
    mycursor.execute("SELECT DISTINCT channels.channel_name \
                 FROM youtube_final.channels \
                 JOIN video ON channels.channel_id = video.channel_id \
                 WHERE YEAR(video.published_at) = 2022")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[name of channels published video on 2022]")
    st.write(df)

#Query 9
elif query_select== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, SEC_TO_TIME(AVG(video.duration)) AS average_duration \
                 FROM youtube_final.video \
                 JOIN channels ON video.channel_id = channels.channel_id \
                 GROUP BY channels.channel_name")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Average video duration for each channels]")
    st.write(df)

#Query 10
elif query_select== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, video.video_tittle, video.Comment_count \
                 FROM youtube_final.video \
                 JOIN channels ON video.channel_id = channels.channel_id \
                 ORDER by (Comment_count) DESC LIMIT 10")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Highest number of comments and their channel name]")
    st.write(df)
        