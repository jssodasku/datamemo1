# YouTubeData Collector 

A wrapper around Google's Python API client to collect data from the [YouTube Data API](https://developers.google.com/youtube/v3). 

## Minimal working example 
### Installation 
Install the package `youtubecollector` by activating your virtual environment of choice and then writing 
```bash
pip install -e .
```
in your terminal at the top of the project directory. 
### API key
- Create a project at https://console.cloud.google.com/apis/ and generate an API key at https://console.cloud.google.com/apis/credentials. Next, activate the YouTube Data v3 API by searching for `YouTube Data API v3` in the search bar and clicking on "Enable API". 

### Code 
Full examples are given in `YouTubeData-Collector/example-notebooks`
#### Data collection
##### Collecting data
```python
from youtubecollector.ytcollector_class import YtEntityData
import pandas as pd 

CHANNELS = {"ria-news": "UCsd4tKHuW5LRIfUwf109Zvg", "sputnik": "UCE8LqmM9zkuzOgaYhakwTdw"}
API_KEY = "YOUR_API_KEY"
cutoff_date = pd.Timestamp("2022-06-06", tz="UTC")
yt_entity_data = YtEntityData(api_key=API_KEY, channels=CHANNELS, cutoff_date=cutoff_date)
yt_entity_data.collect_data_channels()

## Output: 
# Comments collected from 0/3 videos for channel = ria-news
# Comments collected from 3/3 videos for channel = ria-news
# ... done
# --------------------------------------------------
# --------------------------------------------------
# Collecting data for channel: sputnik
# Comments collected from 0/165 videos for channel = sputnik
# Comments collected from 50/165 videos for channel = sputnik
# Comments collected from 100/165 videos for channel = sputnik
# Comments collected from 150/165 videos for channel = sputnik
# Comments collected from 165/165 videos for channel = sputnik
# ... done
# --------------------------------------------------
```
##### Data stored in `YtEntityData` object
```python
print(
    yt_entity_data.df_m,  # dataframe with metadata about videos collected
)

# Output:
#      video_ids durations dimensions  \
# 0  hbN2mrq_skM       P0D         2d   
# 1  VwzWe1MnRkc  PT14M33S         2d   
# 2  3kFvsiDMdmY  PT20M55S         2d   
# 3  CcViGhYj-ck  PT24M41S         2d   
# 4  bErUNL11VcA   PT14M5S         2d   

#                                  region_restrictions view_counts like_counts  \
# 0  [AD, AE, AF, AG, AI, AL, AM, AO, AQ, AR, AS, A...           0           0   
# 1  [AD, AE, AF, AG, AI, AL, AM, AO, AQ, AR, AS, A...           0           0   
# 2  [AD, AE, AF, AG, AI, AL, AM, AO, AQ, AR, AS, A...           0           0   
# 3  [AD, AE, AF, AG, AI, AL, AM, AO, AQ, AR, AS, A...           0           0   
# 4  [AD, AE, AF, AG, AI, AL, AM, AO, AQ, AR, AS, A...           1           0   

#   favorite_counts comment_counts          upload_dates  \
# 0               0              0  2022-06-12T17:35:58Z   
# 1               0              0  2022-06-12T09:48:44Z   
# 2               0              0  2022-06-12T05:54:32Z   
# 3               0              0  2022-06-11T17:23:20Z   
# 4               0              0  2022-06-11T13:01:56Z   

#                                               titles  \
# 0  Гаспарян: помилует ли Пушилин осуждённых в ДНР...   
# 1  Журавлев: выход из Болонской системы - это воз...   
# 2  Освобождение промзоны Северодонецка, топливные...   
# 3  Жесткое послание Анкары для Швеции и Финляндии...   
# 4  Зубец о том, почему России необходимо стать «т...   

#                                         descriptions  
# 0  Наш канал в Telegram: https://t.me/sputniklive...  
# 1  Российское образование находится на пороге реф...  
# 2  Наш канал в Telegram: https://t.me/sputniklive...  
# 3  Наш канал в Telegram: https://t.me/sputniklive...  
# 4  Почему Россия должна вернуться в начало 2000-х...  
```


