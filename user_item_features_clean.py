import pandas as pd
import numpy as np

'''
clean the usr_item_features.csv
    create 8 columns for features
    clean features columns and convert to int
    create 1 column for zpid_hash
    clean zpid_hash
output: cleaned data frame saved as csv
'''
df_chunck = pd.read_csv("user_item_features.csv", chunksize=500000)
for data in df_chunck:
    df = data
    break

df = df.assign(features=df["features"].str.split(")")).explode("features")
df["features"] = df['features'].apply(lambda x: x.lstrip(",Row") )
df[['zpid_hash','feature1','feature2', 'feature3', 'feature4', 'feature5', 'feature6', 'feature7', 'feature8']] = df.features.str.split(",", expand=True)
df["zpid_hash"] = df['zpid_hash'].apply(lambda x: x.lstrip(" Row(") )

df = df.dropna()

df["feature1"] = df['feature1'].apply(lambda x: x.lstrip(" feature1=") )
df["feature2"] = df['feature2'].apply(lambda x: x.lstrip(" feature2=") )
df["feature3"] = df['feature3'].apply(lambda x: x.lstrip(" feature3=") )
df["feature4"] = df['feature4'].apply(lambda x: x.lstrip(" feature4=") )
df["feature5"] = df['feature5'].apply(lambda x: x.lstrip(" feature5=") )
df["feature6"] = df['feature6'].apply(lambda x: x.lstrip(" feature6=") )
df["feature7"] = df['feature7'].apply(lambda x: x.lstrip(" feature7=") )
df["feature8"] = df['feature8'].apply(lambda x: x.lstrip(" feature8=") )

df = df.replace('',np.nan)
df = df.dropna()

df["feature1"] = df['feature1'].apply(lambda x: int(x))
df["feature2"] = df['feature2'].apply(lambda x: int(x))
df["feature3"] = df['feature3'].apply(lambda x: int(x))
df["feature4"] = df['feature4'].apply(lambda x: int(x))
df["feature5"] = df['feature5'].apply(lambda x: int(x))
df["feature6"] = df['feature6'].apply(lambda x: int(x))
df["feature7"] = df['feature7'].apply(lambda x: int(x))
df["feature8"] = df['feature8'].apply(lambda x: int(x))

df["zpid_hash"] = df['zpid_hash'].apply(lambda x: x.lstrip("[Row") )
df["zpid_hash"] = df['zpid_hash'].apply(lambda x: x.lstrip("(") )
df["zpid_hash"] = df['zpid_hash'].apply(lambda x: x.lstrip("zpid_hash='") )
df["zpid_hash"] = df['zpid_hash'].apply(lambda x: x.rstrip("'") )

df = df.drop(columns = ['features'])

df.to_csv("user_session_cleaned.csv")
