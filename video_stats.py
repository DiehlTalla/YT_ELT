import requests 
import json
from datetime import date
import os

from dotenv import load_dotenv 

load_dotenv(dotenv_path=" ./.env")
API_KEY = "AIzaSyCsd9sgfm7oyrNyeYevYwBYWoyEIv0kVPA"

CHANNEL_HANDLE  = "MrBeast"
maxResults = 50

def get_playlist_id():
    """Récupère la playlist automatique 'uploads' d'une chaîne YouTube via son handle."""
    try:
        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        )

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        print("Playlist ID :", playlist_id)
        return playlist_id

    except requests.exceptions.RequestException as e:
        raise e


def get_video_ids(playlist_id):
    """Récupère tous les videoId contenus dans une playlist YouTube."""
    video_ids = []
    page_token = None

    try:
        while True:
            url = (
                f"https://youtube.googleapis.com/youtube/v3/playlistItems"
                f"?part=contentDetails&maxResults={maxResults}"
                f"&playlistId={playlist_id}&key={API_KEY}"
            )

            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
    

def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):

      for video_id in range(0,len(video_id_lst), batch_size):
          yield video_id_lst[video_id: video_id +batch_size]

    try:

     for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}=1&key={API_KEY}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video": video_id,
                    "title":snippet['title'],
                    "publishedAt":snippet['publishedAt'],
                    "duration":contentDetails['duration'],
                    "viewcount":statistics.get('viewCount', None),
                    "likeCount":statistics.get('likeCount', None),
                    "commentCount":statistics.get('commentCount', None),

            }    

            extracted_data.append(video_data)
     return extracted_data

    except requests.exceptions.RequestException as e:

        raise e
    
def save_to_json(extracted_data):
    file_path = f"./data/data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data,json_outfile, indent=4, ensure_ascii=False)




if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    