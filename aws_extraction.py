import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_mannager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_mannager)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_URI = playlist_link.split('/')[-1]
    
    data = sp.playlist_tracks(playlist_URI)
    
    client = boto3.client('s3')
    
    client.put_object(
        Bucket="spotify-etl-mausham",
        Key="raw_data/to-process/" + str(datetime.now()) + ".json",
        Body=json.dumps(data)
        )
    
    
    
