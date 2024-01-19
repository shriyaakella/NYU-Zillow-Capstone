import pandas as pd

chunks = []
for chunked_df in pd.read_csv("~/Downloads/search_sessions.csv", 
                                               header=0,
                                               chunksize=10000):
    chunks.append(chunked_df.copy())
    break

df = chunks[0]
df['Dates'] = pd.to_datetime(df['timestamp_session']).dt.date
df['Time'] = pd.to_datetime(df['timestamp_session']).dt.time

for i in range(len(df)):
    df['Time'][i] = df['Time'][i].hour

for i in range(len(df)):
    df.impressions[i]= df.impressions[i].split('Row')[1:]

for i in range(len(df)):
    for j in range(len(df.impressions[i])):
        df.impressions[i][j] = df.impressions[i][j].rstrip('), "').lstrip("(").split(',')

df = df.explode('impressions')
df1 = df['impressions'].apply(pd.Series)

df_final = pd.concat([df, df1], axis=1)
df_final.columns.values[8:] =["order", "zpid", "submit", "fav", "click" ]

df_final.iloc[:,-1] = df_final.iloc[:,-1].str.replace('is_clicked=', '')
df_final.iloc[:,-2] = df_final.iloc[:,-2].str.replace('is_favorite=', '')
df_final.iloc[:,-3] = df_final.iloc[:,-3].str.replace('is_submit=', '')
df_final.iloc[:,-4] = df_final.iloc[:,-4].str.replace('zpid_hash=', '')
df_final.iloc[:,-5] = df_final.iloc[:,-5].str.replace('order_in_exposed=', '')

df_final.click = df_final.click.str.rstrip(')]')
df_final.fav = df_final.fav.str.rstrip(')]')
df_final.submit = df_final.submit.str.rstrip(')]')

df_final.click = df_final.click.replace({' True': 1, ' False': 0})
df_final.submit = df_final.submit.replace({' True': 1, ' False': 0})
df_final.fav = df_final.fav.replace({' True': 1, ' False': 0})

df_final.zpid = df_final.zpid.str[:-1]
df_final.zpid = df_final.zpid.str[2:]

df_final.drop('impressions', axis = 1, inplace=True)

df_final.to_csv("~/Downloads/search_session_cleaned.csv")