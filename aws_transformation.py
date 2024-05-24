import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def album(data):
    album_list =[]
    for row in data['items']:
        album_name = row['track']['album']['name']
        album_id = row['track']['album']['id']
        album_release_date = row['track']['album']['release_date']
        album_url = row['track']['album']['external_urls']['spotify']
        album_total_tracks = row['track']['album']['total_tracks']
    
        album_dictionary = {"album_name": album_name, "album_id":album_id, "album_release_date": album_release_date, "album_url": album_url, 
                            "album_total_tracks": album_total_tracks}
        album_list.append(album_dictionary)
        
    return album_list
    

def artist(data):
    artist_list = []
    for row in data['items']:
        artist_name = row['track']['artists'][0]['name']
        artist_id = row['track']['artists'][0]['id']
        artist_url = row['track']['artists'][0]['href']
    
        artist_dict = {"artist_name": artist_name, "artist_id": artist_id, "artist_url": artist_url}
        artist_list.append(artist_dict)
        
    return artist_list
    

def song(data):
    song_list = []
    for row in data['items']:
        song_id= row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['artists'][0]['id']
    
        song_dict = {"song_id": song_id, "song_name": song_name, "song_duration": song_duration, "song_popularity": song_popularity, 
                     "song_added": song_added,
                     "album_id": album_id, "artist_id": artist_id}
        
        song_list.append(song_dict)
        
    return song_list


def lambda_handler(event, context):
    s3 = boto3.client('s3') 
    Bucket = "spotify-etl-mausham"
    Key = "raw_data/to-process/"
    
    
    spotify_data_list = []
    spotify_keys = []
    
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file["Key"]
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key = file_key)
            content = response["Body"]
            required_data = json.loads(content.read())
            
            spotify_data_list.append(required_data)
            spotify_keys.append(file_key)
            
    
    for data in spotify_data_list:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)
        
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        album_df['album_release_date']= pd.to_datetime(album_df['album_release_date'])
        song_df['song_added']= pd.to_datetime(song_df['song_added'])
        
    
        album_key = "transformed_data/album-data/album_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key= album_key, Body =album_content)
        
        artist_key = "transformed_data/artist-data/artist_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index= False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key= artist_key, Body =artist_content)
        
        song_key = "transformed_data/song-data/song_" + str(datetime.now()) + ".csv"
        song_Buffer = StringIO()
        song_df.to_csv(song_Buffer, index = False)
        song_content = song_Buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key= song_key, Body=song_content)
        
        
    s3_resources = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key': key
        }
        
        s3_resources.meta.client.copy(copy_source, Bucket, 'raw_data/already-processed/' + key.split("/")[-1])
        s3_resources.Object(Bucket, key).delete()
        
    
            
            