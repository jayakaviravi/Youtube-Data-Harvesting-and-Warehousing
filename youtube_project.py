
#API libraries
from googleapiclient.discovery import build

#MangoDB libraries
import pymongo
from pymongo import MongoClient

#SQL libraries
import mysql.connector as sql

# pandas libraries
import pandas as pd
from pandas import Timestamp
import time

# file handling libraries
import re

#dashboard libraries
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go

#setting page configuration

img=Image.open("C:\\Users\\JAYAKAVI\\Downloads\\images.png")
st.set_page_config(page_title="Scrapping Youtube data", 
                    page_icon=img, 
                    layout="wide",
                    initial_sidebar_state="auto") 

st.title(':red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]')

with st.sidebar:
    selected = option_menu(None, ["Application Details","Scrap datas from Youtube and transform datas to MongoDB and My SQL"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="horizontal",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "white"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "3000px"},
                                   "nav-link-selected": {"background-color": "violet"}})


if selected=="Application Details":
    
     st.markdown('#### :green[Skills take away From This Project:] Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Management using MongoDB and My SQL ')
     
     st.markdown("#### :green[Domain] : Social Media" )
     
     st.markdown("#### :green[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
 
#API connection

def api_connect():
    api_key='AIzaSyCXmCQt3u5m-l-9MqRQjSU_MCSuA3uGX24'
    api_service_name='youtube'
    api_version='v3'
    youtube=build(api_service_name,api_version,developerKey=api_key)
    return youtube

youtube=api_connect()

# SQL connection:
    
mydb=sql.connect(host='127.0.0.1',
                user='root',
                password='test',
                port='3306'
                )
cur=mydb.cursor(buffered=True)
cur.execute('create database if not exists youtube_data')


# Define a function to convert duration
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))


#get channel details
def get_Channel_information(Channel_Id):
    
    request=youtube.channels().list(part='snippet,contentDetails,statistics',id=Channel_Id)
    response=request.execute()
    for i in response['items']:
        
        channel_data=dict(channel_name=i['snippet']['title'],
                  channel_id=i['id'],
                  channel_description=i['snippet']['description'],
                  channel_views=i['statistics']['viewCount'],
                  channel_subcribers=i['statistics']['subscriberCount'],
                  channel_total_videos=i['statistics']['videoCount'],
                  channel_published=i['snippet']['publishedAt'].replace('Z',''),
                  channel_playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                  channel_country=i['snippet'].get('country')
                  )
    
        return channel_data

#get video_id details

def get_channel_videos_ids(Channel_Id):
    video_ids=[]
    request = youtube.channels().list(id=Channel_Id,part='contentDetails')
    response=request.execute()
    
    playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    nextpagetoken = None
    
    while True:
        request = youtube.playlistItems().list(part='snippet',playlistId=playlist_Id,maxResults=50,pageToken=nextpagetoken)
        res=request.execute()
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        nextpagetoken = res.get('nextPageToken')
        
        if nextpagetoken is None:
            break
    return video_ids

#get video stats details

def get_video_information(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:

        response=request = youtube.videos().list(part='snippet,contentDetails,statistics',id=video_id ).execute()
        for i in response['items']:
            data=dict(channel_name=i['snippet']['channelTitle'],
                    channel_id=i['snippet']['channelId'],
                    v_id=i['id'],
                    v_title=i['snippet']['title'],
                    v_description=i['snippet']['description'],
                    v_published=i['snippet']['publishedAt'].replace('Z',''),
                    v_thumbnails=i['snippet']['thumbnails']['default']['url'],
                    v_tags=','.join(i['snippet'].get('tags',['NA'])),
                    v_duration= convert_duration(i['contentDetails']['duration']),
                    v_definition=i['contentDetails']['definition'],
                    v_caption=i['contentDetails']['caption'],
                    v_views=int(i['statistics']['viewCount']),
                    v_likes=int(i['statistics'].get('likeCount',0)),
                    v_comments=int(i['statistics'].get('commentCount',0)))
                   
            video_data.append(data)
    
            
    return video_data

#get comment details

def get_comment_information(Video_ids):
    comment_data=[]
    for video_id in Video_ids:
        try:
            request = youtube.commentThreads().list(part='snippet,replies',videoId=video_id,maxResults=100)
            response=request.execute()

            for i in response['items']:
                data=dict(v_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                          com_id=i['snippet']['topLevelComment']['id'],
                          com_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                          com_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          com_published=i['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''),
                          com_likes=int(i['snippet']['topLevelComment']['snippet']['likeCount']),
                          com_replies=int(i['snippet']['totalReplyCount']))
                comment_data.append(data)
        except:
            pass
    return comment_data

#Function to get channel name

def channel_names():   
    ch_name = []
    collection1=db.Channel_details
    for i in collection1.find():
        ch_name.append(i['channel_information']["channel_name"])
    return ch_name


#extraction  datas  from  youtube:

if selected=="Scrap datas from Youtube and transform datas to MongoDB and My SQL":

    tab1,tab2,tab3,tab4,tab5,tab6,tab7= st.tabs(["Extract datas from Youtube"," Channel details", "video details", "comment details","Transform datas to MongoDB ", "Transform datas to My SQL from MongoDB","Data Analysis"])

    with tab1:

        st.header(':violet[Data collection zone]:sparkles:',divider='rainbow') 

        st.write('This zone collects data from Youtube using channel id :sunglasses:')

        st.write(':red[ENTER CHANNEL ID :]')

        ch_id = st.text_input(":black[Goto youtube homepage >search channel name> Goto channel's home page > Right click > View page source > Find and copy the channel_id]").split(',')

        if ch_id and st.button(":green[Extract Data from youtube]"):

            with tab2:
                c_details = get_Channel_information(ch_id)
                st.table(c_details)
                

            with tab3:
                v_id_details=get_channel_videos_ids(ch_id)
                v_details=get_video_information(v_id_details)
                st.table(v_details)
                

            with tab4:
                v_id_details=get_channel_videos_ids(ch_id)
                com_details=get_comment_information(v_id_details)       
                st.table(com_details)

            st.success("channel,video,comment details uploaded successfully...")
                
            st.balloons()

 # upload datas to MongoDB:

                
    with tab5:

        st.header(':violet[Data Migrate to MongoDB]:sparkles:',divider='rainbow') 

        st.write('This zone extracts data from Youtube and store it in the :orange[MongoDB] database :sunglasses:')

#Create database in Mongo DB:
        
        client = pymongo.MongoClient("mongodb://localhost:27017")
        db = client["youtube_data"]

        ch_id = st.text_input("Enter channel id transform datas to MongoDB")
        
        if st.button(":blue[Upload datas to MongoDB]"):
        
            channel_ids=[]
            db=client['youtube_data']
            collection1=db['Channel_details']

            for Ch_data in collection1.find({},{'_id':0,'channel_information':1}):
                channel_ids.append(Ch_data['channel_information']['channel_id'])

            if ch_id in channel_ids:
                st.success('The given channel id is already exists')
            
            else:
                        
                c_details=get_Channel_information(ch_id)
                v_id_details=get_channel_videos_ids(ch_id)
                v_details=get_video_information(v_id_details)
                com_details=get_comment_information(v_id_details)

                collection1 = db['Channel_details']
                collection1.insert_one({'channel_information':c_details}) 

                collection2 = db['video_details']
                collection2.insert_one({'video_information':v_details}) 

                collection3=db['comment_details']
                collection3.insert_one({'comment_information':com_details})

                st.success("Datas uploaded to MongoDB successfully.....")

                st.balloons()


    #  upload data to My SQL from MongoDB:
    
    with tab6:

        st.header(':violet[Data Migrate to SQL]:sparkles:',divider='rainbow') 

        st.write('This zone extracts data from MongoDB and store it in the :orange[My SQL] :sunglasses:')

        names=channel_names()   
        selected_channel = st.selectbox('Select a channel', options=names,index=None,placeholder="You selected")
        
# Function to create channel table:
        
        def insert_channel_table():   
            mydb=sql.connect(host='127.0.0.1',
                            user='root',
                            password='test',
                            database='youtube_data',
                            port='3306'
                            )
            cur=mydb.cursor(buffered=True)
            drop_query=''' drop table if exists channels'''
            cur.execute(drop_query)
            mydb.commit()

            try:
                create_channel_table='''create table if not exists youtube_data.channels(channel_name varchar(225),
                                                                                            channel_id varchar(225) primary key,
                                                                                            channel_description text,
                                                                                            channel_views int,
                                                                                            channel_subcribers int,
                                                                                            channel_total_videos int,
                                                                                            channel_published timestamp,
                                                                                            channel_playlist_id varchar(225),
                                                                                            channel_country varchar(50))'''
                cur.execute(create_channel_table)
                mydb.commit()
            except:
                st.write('channel table created')
            
            channel_list=[]
            db=client['youtube_data']
            collection1=db['Channel_details']
            for channel_data in collection1.find({},{'_id':0,"channel_information":1}):
                channel_list.append(channel_data["channel_information"])
            df_channel=pd.DataFrame(channel_list)


            for i,row in df_channel.iterrows():
                channel_query='''insert into youtube_data.channels values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                cur.execute(channel_query,tuple(row))
                mydb.commit()

#Function to create Video table:
        
        def insert_video_table():
            mydb=sql.connect(host='127.0.0.1',
                            user='root',
                            password='test',
                            database='youtube_data',
                            port='3306',
                            )
            cur=mydb.cursor(buffered=True)
            drop_query=''' drop table if exists videos'''
            cur.execute(drop_query)
            mydb.commit()
            
            try:
                create_video_table='''create table if not exists youtube_data.videos(channel_name varchar(225),
                                                                                        channel_id varchar(225),
                                                                                        v_id varchar(225) primary key,
                                                                                        v_title varchar(225),
                                                                                        v_description text,
                                                                                        v_published timestamp,
                                                                                        v_thumbnails varchar(225),
                                                                                        v_tags text,
                                                                                        v_duration time,
                                                                                        v_caption varchar(100),
                                                                                        v_definition varchar(100),
                                                                                        v_views bigint,
                                                                                        v_likes bigint,
                                                                                        v_comments bigint
                                                                                        )'''

                cur.execute(create_video_table)
                mydb.commit()
            except:
                st.write("video table created")


            video_list=[]
            db=client['youtube_data']
            collection2=db['video_details']
            for video_data in collection2.find({},{'_id':0,"video_information":1}):
                for i in range(len(video_data['video_information'])):
                    video_list.append(video_data["video_information"][i])
            df_video=pd.DataFrame(video_list)
            
            for i,row in df_video.iterrows():
                video_query='''insert into youtube_data.videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                cur.execute(video_query,tuple(row))
                mydb.commit()

#Function to get comments table: 
        def insert_comment_table():
            mydb=sql.connect(host='127.0.0.1',
                            user='root',
                            password='test',
                            database='youtube_data',
                            port='3306'
                            )
            cur=mydb.cursor(buffered=True)
            drop_query=''' drop table  if exists comments'''
            cur.execute(drop_query)
            mydb.commit()
            
            try:
                create_comment_table='''create table if not exists youtube_data.comments(v_id varchar(225),
                                                                                            com_id varchar(225) primary key,
                                                                                            com_text text,
                                                                                            com_author varchar(225),
                                                                                            com_published timestamp,
                                                                                            com_likes bigint,
                                                                                            com_replies int)'''

                cur.execute(create_comment_table)
                mydb.commit()
            except:
                st.write('comment table created')
            
            comment_list=[]
            db=client['youtube_data']
            collection3=db['comment_details']
            
            for comment_data in collection3.find({},{'_id':0,"comment_information":1}):
                for j in range(len(comment_data['comment_information'])):
                    comment_list.append(comment_data["comment_information"][j])
            df_comment=pd.DataFrame(comment_list)


            for i,row in df_comment.iterrows():
                comment_query='''insert into youtube_data.comments values(%s,%s,%s,%s,%s,%s,%s)'''
                cur.execute(comment_query,tuple(row))
                mydb.commit()
        
        def create_tables():
            insert_channel_table()
            insert_video_table()
            insert_comment_table()
            return "Tables created successfully"
        

        
        if st.button(':blue[Migrate datas to My SQL]'):
            try:
                tables=create_tables()
                st.success("Datas Transformed to My SQL successfully...")
                st.balloons()
            except:
                st.error('not uploaded')
        if st.button(":green[Re Migrate]"):
            mydb=sql.connect(host='127.0.0.1',
                    user='root',
                    password='test',
                    database='youtube_data',
                    port='3306'
                    )
            cur=mydb.cursor(buffered=True)

#  To fetch channel names of channels available in MySQL Database
            cur.execute('''SELECT channel_name FROM youtube_data.channels''')
            fetched_channelnames = cur.fetchall()
            list_fetched_channelnames = []
            for i in fetched_channelnames:
                list_fetched_channelnames.append(*(list(i)))
            
            if selected_channel in list_fetched_channelnames:
                st.success('The given channel details already exists')
            else:
                st.success('The given channel details not exists')
        
        
    with tab7:
        
        st.header(':violet[SQL queries]',divider='rainbow')
        st.write ('This zone specifies collection of data based on questions given in selectbox')
        
        mydb_question=sql.connect(host='127.0.0.1',
                        user='root',
                        password='test',
                        database='youtube_data',
                        port='3306'
                        )
        cur=mydb_question.cursor()

        Question=st.selectbox("select your question",('1. What are the names of all the videos and their corresponding channels?',
                                                    '2. Which channels have the most number of videos, and how many videos do they have?',
                                                    '3. What are the top 10 most viewed videos and their respective channels?',
                                                    '4. How many comments were made on each video, and what are their corresponding video names?',
                                                    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                                    '8. What are the names of all the channels that have published videos in the year 2022?',
                                                    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),index=None,placeholder="Select Question...")


        st.write('You selected:', Question)

        #question:1

        if Question=='1. What are the names of all the videos and their corresponding channels?':
            
            
            cur.execute('''select channel_name as Channel_name,v_title as video_title from videos;''')
            result_1 = cur.fetchall()
            df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
            st.write(df1)  

        #question:2

        elif Question=='2. Which channels have the most number of videos, and how many videos do they have?':
            
            col1,col2=st.columns(2)

            with col1:

                cur.execute('''select channel_name as Channel_name,channel_total_videos as number_of_videos from channels order by channel_total_videos desc;''')
                result_2 = cur.fetchall()
                df2 = pd.DataFrame(result_2, columns=['Channel Name', 'number of videos']).reset_index(drop=True)
                st.write(df2)
            
            with col2:
                fig1 = px.pie(df2, values='number of videos', names='Channel Name',color_discrete_sequence=px.colors.sequential.RdBu)
                fig1.update_layout(autosize=False, width=600,height=500)
                st.plotly_chart(fig1,use_container_width=True)

        #question:3

        elif Question=='3. What are the top 10 most viewed videos and their respective channels?':
            
            col1,col2=st.columns(2)
            
            with col1:
                cur.execute('''select channel_name as Channel_name,v_title as video_title,v_views as video_views from videos where v_views is not null order by v_views  desc limit 10;''')
                result_3 = cur.fetchall()
                df3 = pd.DataFrame(result_3, columns=['Channel name', 'video name','video views ']).reset_index(drop=True)
                st.write(df3)
            
            with col2:
                fig2 = px.bar(df3, y='video views ', x='video name', color='Channel name',orientation='v',text_auto='.3s', title="Top 10 most viewed videos")
                fig2.update_traces(textfont_size=12,marker_color=px.colors.diverging.RdYlBu)
                fig2.update_layout(width=1000, height=700,title_font_color='red',title_font=dict(size=18))
                st.plotly_chart(fig2,use_container_width=True)
                    

        #question:4

        elif Question=='4. How many comments were made on each video, and what are their corresponding video names?':

            cur.execute('''select v_id as video_id,v_title as video_title, v_comments as number_of_comments from videos where v_comments is not null;''')
            result_4 = cur.fetchall()
            df4 = pd.DataFrame(result_4, columns=[ 'video id', 'video name','comment count']).reset_index(drop=True)
            st.write(df4)

        #question:5

        elif Question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            
            col1,col2=st.columns(2)

            with col1:
                cur.execute('''select channel_name as Channel_name,v_title as video_title ,v_likes as count_of_likes from videos v_likes where v_likes is not null order by v_likes desc''')
                result_5 = cur.fetchall()
                df5 = pd.DataFrame(result_5, columns=[ 'channel name', 'video name','likes count']).reset_index(drop=True)
                st.write(df5)
            with col2:

                fig3 = px.bar(df5,x='channel name',y='likes count',orientation='v',color_discrete_sequence=px.colors.sequential.Rainbow,title='Highest number of likes in each videos')
                fig3.update_layout(width=700, height=700,title_font_color='red',title_font=dict(size=20))
                st.plotly_chart(fig3,use_container_width=True)
        
        #question:6

        elif Question=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            

            cur.execute('''select v_title as video_title,v_likes as video_likes from videos;''')
            result_6 = cur.fetchall()
            df6 = pd.DataFrame(result_6, columns=['video name','likes count']).reset_index(drop=True)
            st.write(df6)

        #question:7   

        elif Question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            
            col1,col2=st.columns(2)

            with col1:
                cur.execute('''select channel_name as Channel_name, channel_views as totalnumber_of_views from channels ;''')
                result_7 = cur.fetchall()
                df7 = pd.DataFrame(result_7, columns=['channel name','total number of views']).reset_index(drop=True)
                st.write(df7)
            
            with col2:
                df= df7.sort_values(by='total number of views', ascending=False)
                fig4 = px.pie(df, values='total number of views', names='channel name', hole=.4,title='Total number of views',color_discrete_sequence=px.colors.diverging.Spectral)
                fig4.update_traces(text=df['channel name'], textinfo='percent+label',texttemplate='%{percent:.2%}', textposition='outside')
                st.plotly_chart(fig4,use_container_width=True)
        #question:8

        elif Question=='8. What are the names of all the channels that have published videos in the year 2022?':
            

            cur.execute('''select channel_name as Channel_name,substr(v_published,1,4) as video_published from videos where v_published like '2022%';''')
            result_8 = cur.fetchall()
            df8 = pd.DataFrame(result_8, columns=['channel name','videos published year']).reset_index(drop=True)
            st.write(df8)

        #question:9

        elif Question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            
            col1,col2=st.columns(2)

            with col1:

                cur.execute('''select channel_name as Channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v_duration)))), '%H:%i:%s') as average_duration from videos group by channel_name;''')
                result_9 = cur.fetchall()
                df9 = pd.DataFrame(result_9, columns=['channel name','average duration']).reset_index(drop=True)
                st.write(df9)

            with col2:
                df_sorted = df9.sort_values(by='average duration', ascending=False)
                df_sorted['average duration'] = pd.to_timedelta(df_sorted['average duration']).dt.total_seconds()
                fig5 = px.pie(df_sorted, names='channel name',values='average duration', hole=0.5,color_discrete_sequence=px.colors.sequential.Rainbow)
                fig5.update_traces(text=df_sorted['channel name'], textinfo='percent+label',texttemplate='%{percent:.2%}', textposition='outside')
                st.plotly_chart(fig5,use_container_width=True)

        #question:10
            
        elif Question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            
            col1,col2=st.columns(2)

            with col1:
                cur.execute('''select channel_name as Channel_name ,v_id as video_id ,v_comments as comment_count  from videos order by v_comments desc limit 10;''')
                result_10= cur.fetchall()
                df10 = pd.DataFrame(result_10, columns=['channel name','video id','count of comments']).reset_index(drop=True)
                st.write(df10)

            with col2:
                fig6 = px.bar(df10, y='count of comments', x='video id', color='channel name',orientation='v',text_auto='.3s', title="Videos have highest number of comments")
                fig6.update_traces(textfont_size=12,marker_color=px.colors.diverging.Spectral)
                fig6.update_layout(width=900, height=500,title_font_color='red',title_font=dict(size=18))
                st.plotly_chart(fig6,use_container_width=True)


        # SQL DB connection close
        mydb_question.close()                       




    

    
    
        



