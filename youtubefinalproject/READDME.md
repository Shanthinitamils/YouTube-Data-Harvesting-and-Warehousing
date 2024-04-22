Youtube Data Harvesting and Warehousing

Introduction
YouTube, the online video-sharing platform, has revolutionized the way we consume and interact with media. Launched in 2005, it has grown into a global phenomenon, serving as a hub for entertainment, education, and community engagement. With its vast user base and diverse content library, YouTube has become a powerful tool for individuals, creators, and businesses to share their stories, express themselves, and connect with audiences worldwide.

This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL using SQLAlchemy then analyse the data and give the results depending on the customer questions.


Developer Guide
1.Tools Install
    Virtual code.
    Jupyter notebook.
    Python 3.12.3
    MySQL.
    SQLAlchemy
    Youtube API key.
2.Requirement Libraries to Install
        
    pip install google-api-python-client, mysql-connector-python, sqlalchemy, pandas, streamlit.

3.Import Libraries
    Youtube API libraries
    import googleapiclient.discovery
    from googleapiclient.discovery import build
    import json
    import re
    import mysql.connector
    import sqlalchemy
    from sqlalchemy import create_engine
    import pandas as pd
    import streamlit as st
Code Process
a)Extract data
    Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.
b) Process and Transform the data
    After the extraction process, takes the required details from the extraction data and transforn it into data frame using pandas.
c)Load data
    After the transformation process we transfer the data to local database(XAMPP) using SQLalchemy.
d)Create Connection
    After transforn data using sqlalchemy we create a connection to database mysql connector
e)Filter the data
    Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
f)Visualization
    Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table.
    

User Guide
Step1.Data Collection:
    Search channel_id, copy and paste on the input box and click the Get data button to collect data using youtube API key
Step2.Data Migrate:
    By clicking the data migrate buttin we transfer all the datas to local database.
Step3:
    By using a Drop down select box we can get a answer for the 10 queries which is in problem statment.
Step4:
    By using a side select button we can visulaize the local database tables in Streamlit.


Link

http://localhost:8501/#youtube-data-harvesting-and-warehousing