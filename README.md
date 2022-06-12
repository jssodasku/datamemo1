# Code to replicate `Censorship on YouTube During Russia’s Invasion of Ukraine DATA MEMO #1` 
## 
## Data collection
The data has been collected using the python package `youtubecollector` located in the `YoutubeData-Collector` subfolder. A minimal working example for how to install and use the package is given in `YouTubeData-Collector/README.md`. 

After having installed the package the data can be recollected by following the approach in the notebook `main-analysis/data-collection.ipynb`. 
In order to download the same data as collected for the data memo multiple runs of the notebook has to be done. This is because of the quota limit that the API imposes on each API key. Each time our API key got a quota limit exceeded error, we waited until the quota limit expired and picked up from where we left in the same notebook. The code saves a log file each time the code is run inside `YouTubeData-Collector/logs`. The log files can be consulted if any errors during the data collection occur. 
## Analysis 
The main analysis is conducted in the R-script `main-analysis/analysis.R`. To run the R-script the main dataset has to be placed inside the `main-analysis/data` folder. 
## Data 
### Channel overview 
An overview of the channels for which we have collected data is given in `main-analysis/data/channels.xlsx`
### Video overview
An overview of the videos for which we have collected data is given in `main-analysis/data/videos.csv`
