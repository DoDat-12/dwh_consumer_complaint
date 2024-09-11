import pandas as pd
import numpy as np

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
# from pandas import json_normalize

# Import Data ----------------------------------------------------------------------------------------------------------

# Consumer Complaint Dataset from MongoDB Atlas
# connection string
uri = "mongodb+srv://tadod:dodat@tadoddwh01.g43jx.mongodb.net/?retryWrites=true&w=majority&appName=TadodDWH01"

# create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    # print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# create database and collection
db = client['customer']
collection = db['complaint']

cursor = collection.find()
data = list(cursor)
complaint_df = pd.DataFrame(data)

print(complaint_df.dtypes())

# Data Transformation --------------------------------------------------------------------------------------------------
# complaints


def complaints(df):
    # just need state for query
    df = df.drop(columns=['Tags', 'ZIP code'])
    df['Date received'] = pd.to_datetime(df['Date received'])

    # possible NaN columns
    nan_columns = ['State', 'Consumer disputed?', 'Company response to consumer', 'Timely response?', 'Submitted via']
    for column in nan_columns:
        nan_mask = df[column].isna()  # true-false series
        num_nan_values = nan_mask.sum()  # number of NaN values
        if num_nan_values > 0:
            # get random value from non NaN values in column
            random_values = np.random.choice(df[column].dropna(), num_nan_values)
            df.loc[nan_mask, column] = random_values

    # condition
    df = df[(df['Date received'] >= '2010-01-01') & (df['Date received'] <= '2020-12-31')]
    df['Date received'] = pd.to_datetime(df['Date received'])

    unique_combinations = df[['Product', 'Sub-product', 'Issue', 'Sub-issue']].drop_duplicates()

    # iterate over the rows with NaN values and fill with unique combinations
    for index, row in df[df.isna().any(axis=1)].iterrows():  # iterrows() loop each row with enumerate
        nan_columns = row.index[row.isna()]  # list of column's name has NaN value
        for column in nan_columns:
            if column in unique_combinations.columns:
                unique_values = unique_combinations[column].values  # list of unique values
                if len(unique_values) > 0:
                    df.at[index, column] = np.random.choice(unique_values)

    # convert boolean column
    map_cols = ['Timely response?', 'Consumer disputed?']
    for column in map_cols:
        df[column] = pd[column].map({'Yes': 1, 'No': 0})

    return df


# Final complaint
complaint_df = complaints(complaint_df)