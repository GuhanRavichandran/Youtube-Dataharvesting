# Youtube-Dataharvesting
Collection of people interest
Project gives insight of people search in youtube and time spent for each video and comments provided by viewers 

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a  SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

**Installation**

To run this project, you need to install the following packages:
```python
pip install google-api-python-client
pip install panda
pip install streamlit
pip install mysql_connector

Data Collection: Utilize the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API,

Store Data to SQL: The application allows users to store datato a SQL data warehouse. Users can choose which channel's data to store. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels,videos, and comments, utilizing SQL queries.

- Channel Analysis:Channel analysis includes insights on channel_name,channel_id,publishedAT,suscriber,views,Total_videos,channel_description and playlist_id Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.


- Video Analysis: Video analysis focuses on channel_name,video_id,title,Duration,views,likes and comments enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.





