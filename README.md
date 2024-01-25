  # Youtube Data Harvesting and Warehousing
  
  ### Introduction
  
  YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.
  
  **Skills take away From This Project**
  
  1. Python scripting
  2. Data Collection
  3. MongoDB
  4. Streamlit
  5. API integration
  6. Data Management using MongoDB  and  My SQL
  
  **Domain**
  - Social media
  
  ### Problem statement
  
  The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
  1.   Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
  2.   Option to store the data in a MongoDB database as a data lake.
  3.   Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
  4.   Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
  5.   Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.
  
  ###  Requirement Libraries to Install

-  pip install google-api-python-client
-  pip install pymongo
-  pip install mysql-connector-python
-  pip streamlit
-  pip install streamlit-option-menu
-  pip install plotly-express
-  pip install regex
-  pip install pandas
-  pip install numpy

### Features

- Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
- Store the retrieved data in a MongoDB database.
- Migrate the data to a SQL data warehouse.
- Analyze and visualize data using Streamlit and Plotly.
- Perform queries on the SQL data warehouse.
- Gain insights into channel performance, video metrics, and more.

### Extraction of data from the YouTube using API key

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

### Storing data in MongoDB

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

### Migrating data to a SQL data warehouse

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

### Data Analysis and questions

The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.

- **Channel Analysis:** Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

- **Video Analysis:** Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.

Utilizing the power of Plotly, users can create various types of charts, including line charts, bar charts, scatter plots, pie charts, and more. These visualizations enhance the understanding of the data and make it easier to identify patterns, trends, and correlations.

The Streamlit app provides an intuitive interface to interact with the charts and explore the data visually. Users can customize the visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis.

With the combined capabilities of Plotly and Streamlit, the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.




