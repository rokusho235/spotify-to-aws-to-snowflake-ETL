import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials as scc
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = scc(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = 'https://open.spotify.com/playlist/0SgyK6Rl58KJH3UGWDhSyc'    # emo playlist
    playlist_URI = playlist_link.split('/')[-1].split('?')[0]
    
    data = sp.playlist_tracks(playlist_URI)
    
    # dumps json files into s3 bucket
    client = boto3.client('s3')
    
    # giving unique filename for the data file to be processed
    filename = 'spotify_raw_' + str(datetime.now()) + '.json'
    
    client.put_object(
        Bucket='spotify-etl-project-joedo',
        Key='raw_data/to_be_processed/' + filename,
        Body=json.dumps(data) #json.dumps converts entire thing into a json string
        )
