import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

# album function

def album_fn(data):
    album_list = []
    for row in data['items']:
        # album ID = https://open.spotify.com/album/1Mo4aZ8pdj6L1jx8zSwJnt
        album_id = row['track']['album']['id']
        # album name
        album_name = row['track']['album']['name']
        # release date of album
        if row['track']['album']['release_date_precision'] == 'day':    # needed to add release_date_precision to make sure it's a date instead of just a year
            album_date = row['track']['album']['release_date']
        else:
            yyyy = int(row['track']['album']['release_date'])           # just setting the date = new year's day of whatever year if there is no date for release date
            album_date = datetime(yyyy,1,1)
        # total tracks from album
        album_trackNo = row['track']['album']['total_tracks']
        # album spotify URL (similar to album ID)
        album_url = row['track']['album']['external_urls']['spotify']
        # album artist
        # album_artist = row['track']['album']['artists'][0]['name']
        
        # creating a dictionary for the above extracted data
        album_element = {
                            'album_id':album_id,
                             'album_name':album_name,
                             'album_date':album_date,
                             'album_trackNo':album_trackNo,
                             'album_url':album_url
                            # ,
                            # 'album_artist':album_artist
                        }
        # adding the top 50 USA song info into the initialized list
        album_list.append(album_element)
    
    return album_list

# artist function

def artist_fn(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {
                                    'artist_id':artist['id'],
                                    'artist_name':artist['name'],
                                    'external_url':artist['href']
                                    }
                    artist_list.append(artist_dict)
    return artist_list

# songs function

def songs_fn(data):
    songs_list = []
    for row in data['items']:
        songs_id = row['track']['id']
        songs_name = row['track']['name']
        songs_duration = row['track']['duration_ms']
        songs_url = row['track']['external_urls']['spotify']
        songs_popularity = row['track']['popularity']
        songs_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
    
        songs_element = {
                            'songs_id':songs_id,
                            'songs_name':songs_name,
                            'songs_duration':songs_duration,
                            'songs_url':songs_url,
                            'songs_popularity':songs_popularity,
                            'songs_added':songs_added,
                            'album_id':album_id,
                            'artist_id':artist_id
                        }
        songs_list.append(songs_element)
    return songs_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-joedo'
    Key = 'raw_data/to_be_processed/'
    
    spotify_data = []
    spotify_keys = []
    
    # loops thru the s3 bucket folder and gathers all the json files // EXTRACTION PROCESS
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':   # checking for json files only
            response = s3.get_object(Bucket = Bucket, Key = file_key)   # pulling the contents from the json file
            content = response['Body']  # pulling just the data from the contents of the json
            json_object = json.loads(content.read())
            
            # storing the data of the files
            spotify_data.append(json_object)
            
            # storing the keys of the files
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list = album_fn(data)
        artist_list = artist_fn(data)
        songs_list = songs_fn(data)
        
        # putting extracted album data into dataframe
        album_df = pd.DataFrame.from_dict(album_list)
        # trimming fat/getting distinct album list
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        # putting extracted artist data into dataframe
        artist_df = pd.DataFrame.from_dict(artist_list)
        # trimming fat/getting distinct artist list
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        # putting extracted song data into dataframe
        songs_df = pd.DataFrame.from_dict(songs_list)
        # trimming fat/getting distinct song list
        songs_df = songs_df.drop_duplicates(subset=['songs_id'])
        
        # converting column/s in album_df to be datetime format
        album_df['album_date'] = pd.to_datetime(album_df['album_date'])
        # converting column/s in songs_df to be datetime format
        songs_df['songs_added'] = pd.to_datetime(songs_df['songs_added'])
        
        songs_key = 'transformed_data/songs/songs_transformed_' + str(datetime.now()) + '.csv'
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer, index=False)  # don't want index because the extra empty/index column will mess with Glue schema formation
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=songs_content)
        
        album_key = 'transformed_data/album/album_transformed_' + str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = 'transformed_data/artist/artist_transformed_' + str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        # moving/copying+deleting json files in the s3 folders
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])   # copying json to the processed folder
        s3_resource.Object(Bucket, key).delete()    # deleting the json since it's now copied over to the processed folder