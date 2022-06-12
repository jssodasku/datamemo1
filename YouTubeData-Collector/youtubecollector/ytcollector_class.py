import time
from typing import Type
from collections import OrderedDict
import logging

import pandas as pd
from googleapiclient.errors import HttpError

from youtubecollector import (
    ytcollector,
    data_utils,
    proc_utils,
    FP_DATA_RAW,
    FP_DATA_PROC,
    FP_DATA_TMP,
)
from youtubecollector.utils import set_logger
from youtubecollector.ytcollector import unpack_http_error

RETRY_TIMES = 10  # Arbitrarily set 

def error_api_method(
    times: int, exceptions: tuple[Type[Exception], ...] = (HttpError,)
):
    """
    Helper decorator to handle http errors returned from the API. 
    Use to decorate the main methods in the class `YtEntityData` below.
    """

    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as exceptions_caught:
                    attempt += 1
                    logging.info(
                        "Exception thrown when attempting to run "
                        f"{func.__name__}, attempt {attempt} of {times}\n."
                        f"Exceptions thrown: {repr(exceptions_caught)}. "
                    )
                    # First argument in method is the initialized object
                    yt_entity_data: YtEntityData = args[0]
                    e_http: HttpError = exceptions_caught
                    # Handle the HTTP error
                    handle_http_error(e_http, yt_entity_data)

        return newfn

    return decorator


class YtEntityData:
    """Class to collect data for youtube channels

    Attributes:
        api_keys: list of api keys
        channels: dictionary with channels and youtube channel ids
        cut_offdate: the start date for the data collection
    """

    def __init__(
        self,
        api_key: str,
        channels: dict,
        cutoff_date: pd.Timestamp,
        use_logger=True,
    ):
        self.use_api_key(api_key)
        self.channels = channels
        self.cutoff_date = cutoff_date
        if use_logger:
            set_logger(prefix="yt-collector")  # Init logger
        self.setup_attributes()

    def setup_attributes(self):
        """Sets up the attributes stored in the object during data collection"""
        # Upload playlist and videos
        self.channel_response = None
        self.upload_responses = None
        self.vid_ids = None
        self.videos_responses = None
        # Dataframes
        self.df_upload_playlist = None
        self.df_videos = None
        self.df_m = None
        # Comments
        self.responses_replies = None
        self.responses_toplevel = None
        self.i = None
        self.temp_id_storage = None
        self.videos_with_comments_disabled = None
        self.comment_threads = None

    def collect_data_channels(self):
        for channel in self.channels:
            print(f"-" * 50)
            print(f"Collecting data for channel: {channel}")
            self.collect_data_channel(channel)
            print(f"... done")
            print(f"-" * 50)

    def collect_data_channel(self, channel):
        logging.info(f"Collecting data for channel: {channel}")
        try:
            self.collect_upload_playlist(channel)
        except KeyError:
            msg = f"Channel {channel} does not have an upload playlist"
            print(msg)
            logging.info(msg)
        else:
            self.collect_videos(channel)
            self.setup_comment_collector()
            self.collect_comments(channel)

    @error_api_method(times=RETRY_TIMES)
    def collect_upload_playlist(self, channel):
        self.channel_response = ytcollector.search_channel_id(
            self.youtube, self.channels[channel]
        )
        upload_playlist_id = ytcollector.get_upload_playlist_id(self.channel_response)
        logging.info(
            f"Found upload-playlist `{upload_playlist_id}` for channel `{channel}`"
        )
        self.upload_responses = ytcollector.query_uploads_date(
            upload_playlist_id, self.youtube, cutoff_date=self.cutoff_date
        )
        data_utils.save_response(
            self.upload_responses, f"{channel}-uploadplaylist-responses", folder=channel
        )
        self.df_upload_playlist = ytcollector.get_dataframe_responses(
            self.upload_responses, ytcollector.get_data_videos_playlist
        )
        data_utils.save_df(
            self.df_upload_playlist, f"{channel}-upload-playlist", folder=channel
        )
        self.vid_ids = self.df_upload_playlist.video_ids.tolist()
        logging.info(
            f"Found {len(self.vid_ids)} number of videos in the upload playlist "
            f"for channel = `{channel}`"
        )

    @error_api_method(times=RETRY_TIMES)
    def collect_videos(self, channel):
        self.videos_responses = ytcollector.query_videos_list(
            self.vid_ids, self.youtube
        )
        data_utils.save_response(
            self.videos_responses, f"{channel}-video-responses", folder=channel
        )
        self.df_videos = ytcollector.get_dataframe_responses(
            self.videos_responses, ytcollector.get_data_videos
        )
        logging.info(
            f"Collected data for {self.df_videos.shape[0]} videos "
            f"for channel = `{channel}`"
        )
        self.df_m = ytcollector.merge_playlist_and_videos(
            self.df_upload_playlist, self.df_videos
        )
        data_utils.save_df(self.df_m, f"{channel}-video-playlist", folder=channel)
        self.get_videoids_to_collect()
        logging.info(
            f"Initialized list of video ids to collect "
            f"for channel = `{channel}`. \nNumber of videos: {len(self.video_ids)}."
        )

    def get_videoids_to_collect(self):
        # Init list of video ids to collect
        self.video_ids = self.df_m.video_ids.tolist()[::-1]
        self.videos_ids_to_collect = list(self.video_ids)  # Copy
        self.collected_video_ids = list()

    @error_api_method(times=RETRY_TIMES)
    def collect_comments(self, channel):
        """
        Collects all comments of the videos specified in the list `video_ids`.
        """
        logging.info(
            f"Collecting comments for channel: {channel}\n"
            f"Current time is {data_utils.get_current_time()}"
        )
        while self.videos_ids_to_collect:
            video_id = self.get_video_id_to_collect()
            self.safe_collect_comments_video(video_id)
            self.bookkeeping_comments(channel, video_id)
        self.save_comments(channel)  # Save comments after last iteration also
        self.flush_all_tmp_storage()  # Flush everything in tmp also

    def setup_comment_collector(self):
        """
        Sets up dictionaries and lists to store data for collecting comments
        """
        self.responses_replies: OrderedDict[str, list[dict]] = OrderedDict()
        self.responses_toplevel: OrderedDict[str, list[dict]] = OrderedDict()
        self.i = len(self.collected_video_ids)
        self.temp_id_storage = list()  # Store video id temporarily
        self.videos_with_comments_disabled = list()

    def get_video_id_to_collect(self) -> str:
        """
        Pops the set of ids to collect and store the id in a buffer for
        retrieval if error occurs
        """
        video_id = self.videos_ids_to_collect.pop()
        self.temp_id_storage.append(video_id)
        return video_id

    def safe_collect_comments_video(self, video_id) -> None:
        """
        Wrapper around `collect_comments_video` made to catch if the
        video specified by `video_id` has disabled comments and the
        youtube API raises a HttpError with `reason` == `commentsDisabled`.
        """
        try:
            self.collect_comments_video(video_id)
        except HttpError as e:
            self.handle_disabled_comments_error(e, video_id)
            return None

    def collect_comments_video(self, video_id):
        """
        Collects all comments for given video_id. 
        It temporarily saves comments, replies in a dict 
        which gets saved in `data/tmp` such that if quotaExceeded error occurs
        in the middle of the collection, it can look up in the dictionary to 
        start from a specific checkpoint after the API have been restored. 
        """
        self.collect_toplevel_comments(video_id)
        self.collect_comment_replies(video_id)

    def collect_toplevel_comments(self, video_id):
        # Init happens inside `ytcollector.query_all_items`
        self.comment_threads = ytcollector.query_all_items(
            video_id, ytcollector.comments_pager, self.youtube
        )
        self.responses_toplevel[  # Empty list if no replies
            video_id
        ] = ytcollector.get_multiple_comment_thread_data(self.comment_threads)

    def collect_comment_replies(self, video_id):
        """
        Collects replies to comments. 
        Tmp storage is handled in function `ytcollector.get_replies_comment_thread`
        """
        # Collect replies to the comment which have replies
        if self.responses_toplevel[video_id]:  # Handle no replies case
            self.comment_thread_responses = ytcollector.get_replies_comment_thread(
                video_id, self.responses_toplevel[video_id], self.youtube
            )
            logging.info(f"Collected comment threads for video {video_id}")
            # Extract and proc the data from the comment thread replies
            self.responses_replies[video_id] = ytcollector.get_comment_reply_data(
                self.comment_thread_responses
            )
        else:
            logging.info(f"No replies for video `{video_id}`")

    def handle_disabled_comments_error(self, e, video_id):
        """
        Handle disabled comments error
        """
        try:
            reason = unpack_http_error(e)
        except Exception:
            # Reraise the HTTP error if we can't unpack error
            raise e
        else:
            if reason == "commentsDisabled":
                self.videos_with_comments_disabled.append(video_id)
                logging.info(
                    f"Caught HttpError for {video_id} with e = `{e}` because of disabled comments"
                )
            else:
                # Re-raise the error and propagate up to the retry decorator
                # if not a commentsDisabled error
                raise e
            return None

    def bookkeeping_comments(self, channel, video_id):
        logging.info(
            f"[{data_utils.get_current_time()}]"
            f"Collected comments for video `{video_id}` on channel `{channel}`"
        )
        # Save files
        if self.i % 50 == 0:
            self.save_comments(channel)  # Save files each time for now ...
            self.flush_all_tmp_storage()  # Flush all
        # Add collected video id to set of collected video ids
        self.collected_video_ids.append(video_id)
        # "Flush the buffer"
        self.temp_id_storage.pop()
        self.i += 1

    def save_comments(self, channel):
        msg = (
            f"Comments collected from {self.i}/{len(self.video_ids)} videos "
            f"for channel = {channel}"
        )
        logging.info(msg)
        print(msg)
        for responses, name in zip(
            [self.responses_toplevel, self.responses_replies],
            [f"{channel}-toplevelcomments", f"{channel}-replies"],
        ):
            data_utils.save_response_comments(
                responses, name, folder=f"{channel}",
            )
        # Save the video ids with disabled comments
        data_utils.save_videos_with_disabled_comments(
            self.videos_with_comments_disabled,
            name=f"{channel}-disabled-comments-videoids",
            folder=f"{channel}",
        )

    def flush_tmp_storage_comments(self, video_id):
        """
        Removes temporary files (if they exist) for toplevel comments 
        and replies for given video_id.
        """
        replies_id = f"{video_id}-replies"
        for id_ in [video_id, replies_id]:
            ytcollector.remove_tmp_data(id_)

    def flush_all_tmp_storage(self):
        """
        Flushes the tmp storage folder.
        """
        for file in FP_DATA_TMP.glob("*"):
            if file.is_file():
                file.unlink()
        logging.info(f"Removed all temporary files in {str(FP_DATA_TMP)}")

    def use_api_key(self, api_key):
        """Build a Ressource object with specified API key"""
        self.current_api_key = api_key
        self.youtube = ytcollector.get_youtube_build(api_key)
        logging.info(f"Using API key: {self.current_api_key}")
        time.sleep(0.5)

    def put_videoid_in_buffer_back(self):
        """
        Function to put current video_id back into list of ids to collect
        before next try.
        Used in the retry decorator
        """
        if hasattr(self, "temp_id_storage"):
            if len(self.temp_id_storage) > 0:
                # If has buffer and buffer non empty
                video_id = self.temp_id_storage.pop()
                self.videos_ids_to_collect.append(video_id)
                print(f"Put id `{video_id}` back in list of video ids to collect")


def handle_http_error(e: HttpError, yt_entity_data: YtEntityData):
    """
    Function to handle errors returned from the API. 
    Retries the function if error is not due to: 
        `quotaExceeded`
        `forbidden`
        `accessNotConfigured`   
    If the reason for the HttpError is one of the three above, 
    the function raises an `AssertionError`. 
    
    Example repr of the error response:
    '<HttpError %s when requesting %s returned "%s". Details: "%s">'
    or
    '<HttpError %s when requesting %s returned "%s">'
    <HttpError 403 when requesting (https://youtube.googleapis.com/youtube
    /v3/playlistItems?part=snippet
    &playlistId=UUFU30dGHNhZ-hkh0R10LhLw
    &maxResults=50&pageToken=EAAaB1BUOkNKQUQ
    &key=AIzaSyD3Isj9g2auBxQpAciFbdkYxTxfpscFICA&alt=json)
    returned "The request cannot be completed because you have exceeded your
    <a href="/youtube/v3/getting-started#quota">quota</a>.".
    Details: "[{'message': 'The request cannot be completed because you have
    exceeded your <a href="/youtube/v3/getting-started#quota">quota</a>.',
    'domain': 'youtube.quota', 'reason': 'quotaExceeded'}]">
    """
    # Insert video_id where occured back into list of ids to collect
    yt_entity_data.put_videoid_in_buffer_back()
    try:
        reason = unpack_http_error(e)  # Unpacks the reason for the error 
    except Exception as e:
        logging.info(
            "Could not unpack the reason of the http error. " f"Error caught: {e}"
        )
        time.sleep(2)
    else:
        if reason == "quotaExceeded":
            msg = f"Quota limit exceeded. Wait until quotaLimit expires."
            raise AssertionError(msg)
        elif reason == "forbidden":
            msg = f"API key `{yt_entity_data.current_api_key}` is suspended. "
            raise AssertionError(msg)
        elif reason == "accessNotConfigured":
            msg = f"API key not activated, activate it at google cloud"
            raise AssertionError(msg)
        else:
            logging.info(
                "Got HTTP error for reason other than quotaExceeded, forbidden,"
                " or accessNotConfigured, sleeping for 5 secs..."
            )
            time.sleep(5)


class YtEntityDataCheckpoint:
    """
    Object to restore YtEntityData object at a given checkpoint. 
    
    Attributes:
        channel: name of channel to restore data for 
        date: date of download of data 
        yt_entity_data: yt data object 
    """

    def __init__(self, channel: str, date: str, yt_entity_data: YtEntityData):
        self.channel = channel
        self.date = date
        self.yt_entity_data = yt_entity_data

    def restore_obj_from_checkpoint(self):
        self.restore_responses()
        self.restore_dfs()
        self.handle_collected_comments()
        return self.yt_entity_data

    def restore_responses(self):
        data_types = ["upload", "video", "toplevel", "replies", "disabled"]
        attr_names = [
            "upload_responses",
            "videos_responses",
            "responses_toplevel",
            "responses_replies",
            "videos_with_comments_disabled",
        ]
        folder = FP_DATA_RAW / self.channel
        for name, attr_name in zip(data_types, attr_names):
            if name == "disabled":  # Temporary name fix
                name = f"{name}*{self.date}"
            else:
                name = f"{self.channel}*{name}*{self.date}"
            file = self.read_file(folder, name)
            setattr(self.yt_entity_data, attr_name, file)

    def restore_dfs(self):
        types_df = ["upload", "video"]
        names_df = ["df_upload_playlist", "df_m"]
        folder = FP_DATA_PROC / self.channel
        for name, attr_name in zip(types_df, names_df):
            file = self.read_file(folder, name, proc=True)
            setattr(self.yt_entity_data, attr_name, file)

    def read_file(self, folder, name, proc=False):
        """
        Wrapper around reading files.
        Set none as attribute if file does not exist.
        """
        try:
            file = proc_utils.get_file(folder, name)
        except AssertionError:
            file = None
        else:
            if proc:
                return pd.read_parquet(file)
            return data_utils.load_pickle_file(file)

    def handle_collected_comments(self):
        self.yt_entity_data.video_ids = self.yt_entity_data.df_m.video_ids.tolist()[
            ::-1
        ]
        self.yt_entity_data.collected_video_ids = list(
            self.yt_entity_data.responses_toplevel.keys()
        )
        self.yt_entity_data.videos_ids_to_collect = [
            video_id
            for video_id in self.yt_entity_data.video_ids
            if video_id not in self.yt_entity_data.collected_video_ids
        ]
        print(
            f"Total number of videoes: {len(self.yt_entity_data.video_ids)}",
            f"Number of collected videos: {(num_collected := len(self.yt_entity_data.collected_video_ids))}",
            f"Number of videos to collect comments from: {len(self.yt_entity_data.videos_ids_to_collect)}",
            sep="\n",
        )
        # Set counter and init buffer
        self.yt_entity_data.i = num_collected
        self.yt_entity_data.temp_id_storage = list()
