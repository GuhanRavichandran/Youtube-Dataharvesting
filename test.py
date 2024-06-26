import mysql.connector
import streamlit as st
import pandas as pd
import googleapiclient.discovery
from datetime import datetime

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyBMXvNN-k7uLBWlbaFYF5b1iMpp_ZO1ehc"
youtube = googleapiclient.discovery.build(
        api_service_name, api_version,developerKey=api_key)
def channel_data(channel_id):
    channel_ids=[]
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",      
    id=channel_id
    )
    response = request.execute()
    published_at_str = response['items'][0]['snippet']['publishedAt']
    
    formatted_published_at = None
    try:
        published_at_dt = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_published_at = published_at_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        # If the first format fails, try without microseconds
        try:
            published_at_dt = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_published_at = published_at_dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Failed to parse {published_at_str} with both datetime formats.")
    data={
          "channel_name":response['items'][0]['snippet']['title'],
          "channel_id":channel_id,
          "publishedAT": formatted_published_at,
          "subscription_count":response['items'][0]['statistics']['subscriberCount'],
          "channel_views":response['items'][0]['statistics']['viewCount'],
          "video_count":response['items'][0]['statistics']['videoCount'],
          "channel_description":response['items'][0]['snippet']['description'],
          "playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
      }
    channel_ids.append(data) 
    return channel_ids
  
  

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="1997",
  auth_plugin='mysql_native_password',
  database='youtubedata'
)
print('connected')
mycursor = mydb.cursor()

st.title(":red[Youtube Data Harvesting]")
st.caption(":green[Insights of people search]:wink:")
st.subheader(':orange[This project demonstrates insights of  people enquiry in youtube]')
st.image(r"C:\Users\Hp\OneDrive\Desktop\demo.py\pile-3d-play-button-logos.jpg", caption="Youtube data collections", use_column_width=True)
import base64

# Function to convert image to base64
def get_base64_of_bin_file(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Path to your image
background_image_path = r'C:\Users\Hp\OneDrive\Desktop\demo.py\youtube image.jpg'

# Convert the image to base64
base64_image = get_base64_of_bin_file(background_image_path)

# CSS to inject
page_bg_img = f"""
<style>
[data-testid="stAppViewContainer"] {{
    background-image: url("data:image/jpg;base64,{base64_image}");
    background-size: cover;
    background-repeat: no-repeat;
    background-attachment: fixed;
}}
</style>
"""

# Inject CSS with markdown
st.markdown(page_bg_img, unsafe_allow_html=True)

# Inject CSS with markdown
st.markdown(page_bg_img, unsafe_allow_html=True)

# Inject CSS with markdown
st.markdown(page_bg_img, unsafe_allow_html=True)
with st.sidebar:
  st.header("Take Away codes")
  st.caption("Python Scripting")
  st.caption("Data Collection")
  st.caption("API integration")
  st.caption("Data Management using MySQL")
  
channel_id=st.text_input(":red[Enter the channel ID]")
if st.button(":red[Store to mysql]"):
  ch_dtls=channel_data(channel_id)
  channel_details = []


# Process each dictionary in the list
  for ch_dtl in ch_dtls:
      # Create a tuple from dictionary values and append to video_data
      data = tuple(ch_dtl.values())
      channel_details.append(data)
      
      
      
      
      sql = """
          INSERT INTO channel_data (channel_name, channel_id,publishedAT, subscriber, views, Total_videos, channel_description, playlist_id)
          VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
      """


      # Define the values to be inserted
      values = channel_details
      
      # Execute the SQL statement with the values
      mycursor.executemany(sql, values)

      # Commit the transaction
      mydb.commit()

      # Close the cursor and connection
      mycursor.close()
      mydb.close()

      print(mycursor.rowcount, "records inserted.")

show_tables=st.radio(":red[select the table for view]",[":red[Channels]",":red[Videos]",":red[comments]"],index=None,)
st.write(":red[your selected table:]",show_tables)

if show_tables =="Channels": 
  st.slider("Total subscribers",200,5000000,(10000,500000)) 
  mycursor.execute("select * from channel_data")
  columndata = mycursor.fetchall()
  channel_table = pd.DataFrame(columndata, columns=mycursor.column_names)
  channels = st.dataframe(channel_table)

if show_tables=="Videos":
  st.slider("Total views",2000,1500000000,(200000000,600000000))
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  videos=st.dataframe(video_table)
  
if show_tables=="comments":
  st.slider("Total comments",0,7000,(100,2500))
  mycursor.execute("select * from comment_detail")
  columndata=mycursor.fetchall()
  comment_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  comments=st.dataframe(comment_table)
  
questions=st.selectbox(":red[select your question]",("1.What are the names of videos and corresponding channels ?",
                                               "2.Which channels have the most number of videos,and how many video do they have ?",
                                               "3.What are the top 10 most viewed videos and their respective channels ?",
                                               "4.How many comments were made on each video,and what are their corresponding video names ?",
                                               "5.which videos have the highest number of likes ,and what are their corresponding channel names ?",
                                               "6.What is the total number of likes of each video,and what are their corresponding video names ?",
                                               "7.What is the total number of views for each channel,and what are their corresponding channel names ?",
                                               "8.What are the names of all the channels that have published video in the year 2022 ?",
                                               "9.What is the average duration of all videos in each channel,and what are their corresponding channel names ?",
                                               "10.Which videos have the highest number of comments,and what are their correponding channel names ?"))

if questions=="1.What are the names of videos and corresponding channels ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["channel_name","title"]]
  st.write(df)
  
if questions=="2.Which channels have the most number of videos,and how many video do they have ?":
  mycursor.execute("select * from channel_detail")
  columndata = mycursor.fetchall()
  channel_table = pd.DataFrame(columndata, columns=mycursor.column_names)
  df=channel_table[["channel_name","Total_videos"]]
  st.write(df)
  
if questions=="3.What are the top 10 most viewed videos and their respective channels ?":
  mycursor.execute("select * from channel_detail")
  columndata = mycursor.fetchall()
  channel_table = pd.DataFrame(columndata, columns=mycursor.column_names)
  df=channel_table[["channel_name","views"]]
  st.write(df)
  
if questions=="4.How many comments were made on each video,and what are their corresponding video names ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["title","comments"]]
  st.write(df)
  
  
if questions=="5.which videos have the highest number of likes ,and what are their corresponding channel names ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["channel_name","title","likes"]]
  st.write(df)
  
if questions=="6.What is the total number of likes of each video,and what are their corresponding video names ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["title","likes"]]
  st.write(df)
  
if questions=="7.What is the total number of views for each channel,and what are their corresponding channel names ?":
  mycursor.execute("select * from channel_detail")
  columndata = mycursor.fetchall()
  channel_table = pd.DataFrame(columndata, columns=mycursor.column_names)
  df=channel_table[["channel_name","views"]]
  st.write(df)
  
if questions=="8.What are the names of all the channels that have published video in the year 2022 ?":
  mycursor.execute("SELECT * FROM channel_data")
  columndata = mycursor.fetchall()
  channel_table = pd.DataFrame(columndata, columns=mycursor.column_names)
  df = channel_table[["channel_name", "publishedAT"]]
  df['publishedAT'] = pd.to_datetime(df['publishedAT'])
  df_2022 = df[df['publishedAT'].dt.year == 2022]
  st.write(df_2022)
  
if questions=="9.What is the average duration of all videos in each channel,and what are their corresponding channel names ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["channel_name","video_id","Duration"]]
  st.write(df)

if questions=="10.Which videos have the highest number of comments,and what are their correponding channel names ?":
  mycursor.execute("select * from video_detail")
  columndata=mycursor.fetchall()
  video_table=pd.DataFrame(columndata,columns=mycursor.column_names)
  df=video_table[["channel_name","title","comments"]]
  st.write(df)
