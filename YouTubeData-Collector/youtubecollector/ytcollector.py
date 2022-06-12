import time
import logging
from typing import Callable
from dataclasses import dataclass
from typing import Union


import googleapiclient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import numpy as np

from youtubecollector import FP_DATA_TMP, data_utils


def get_youtube_build(api_key):
    """
    Returns a Resource object for interacting with the youtube API
    based on the input API key.
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube


def unpack_http_error(e: HttpError):
    """
    Unpacks a HttpError from the googleapiclient and returns
    the reason for the error.
    """
    error_details = e.error_details
    element_error_dict = error_details[0]
    reason = element_error_dict["reason"]
    return reason


def search_channel_id(youtube, channel_id):
    """Returns a channel response object with id of upload playlist"""
    request = youtube.channels().list(part="contentDetails", id=channel_id)
    response = request.execute()
    return response


def get_upload_playlist_id(channel_response):
    """
    Returns the playlist id of the upload playlist based on
    the response returned by the `search_channel_id` function.
    """
    content_details = channel_response["items"][0]["contentDetails"]
    upload_playlist_id = content_details["relatedPlaylists"]["uploads"]
    return upload_playlist_id


def pager_snippet(playlist_id, youtube, page_token=None):
    """
    Function to page through a given channels upload playlist.
    """
    request = youtube.playlistItems().list(
        part="snippet", playlistId=playlist_id, maxResults=50, pageToken=page_token
    )
    response = request.execute()
    return response


def filter_last_items(items: list[dict], cutoff_date: pd.Timestamp) -> list[dict]:
    """Filters video items for videos uploaded too early"""
    filtered_items = [
        item
        for item in items
        if not is_uploaded_before(get_timestamp(item), cutoff_date=cutoff_date)
    ]
    return filtered_items


def query_uploads_date(
    playlist_id: str,
    youtube: googleapiclient.discovery.Resource,
    cutoff_date: pd.Timestamp,
    verbose=False,
):
    """
    Queries the upload playlist until the last video queried is uploaded
    before the `cutoff_date` specified.
    """
    responses = list()
    videos_uploaded_before_date = False
    next_page_token = None
    while not videos_uploaded_before_date:
        response = pager_snippet(playlist_id, youtube, page_token=next_page_token)
        responses.append(response)
        try:
            next_page_token = response["nextPageToken"]
        except KeyError:  # No next page token
            next_page_token = None
            break
        else:
            ts_last_item = get_timestamp(response["items"][-1])
            if verbose:
                print(f"Date of last item is: {ts_last_item}")
            if is_uploaded_before(ts_last_item, cutoff_date=cutoff_date):
                if verbose:
                    print(
                        "Last item from API is uploaded before specified date. "
                        "Breaking out of while loop"
                    )
                videos_uploaded_before_date = True
            time.sleep(0.5)
    # Filter the last response object for the videos uploaded too long ago
    responses[-1]["items"] = filter_last_items(
        responses[-1]["items"], cutoff_date=cutoff_date
    )
    return responses


def get_timestamp(item: dict) -> pd.Timestamp:
    ts = pd.Timestamp(item["snippet"]["publishedAt"])
    return ts


def is_uploaded_before(ts: pd.Timestamp, cutoff_date: pd.Timestamp) -> bool:
    """Checks if timestamp is 1 month before the start of the war"""
    return (ts + pd.DateOffset(months=1)) < cutoff_date


def get_data_videos_playlist(response):
    """Returns video data from a video response from the upload playlist"""
    items = response["items"]
    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in items]
    upload_dates = [item["snippet"]["publishedAt"] for item in items]
    titles = [item["snippet"]["title"] for item in items]
    descriptions = [item["snippet"]["description"] for item in items]
    data = {
        "video_ids": video_ids,
        "upload_dates": upload_dates,
        "titles": titles,
        "descriptions": descriptions,
    }
    return data


def get_dataframe_responses(responses, response_data_func):
    data_dicts = [response_data_func(response) for response in responses]
    df = get_dataframe_dicts(data_dicts)
    return df


def get_dataframe_dicts(data_dicts) -> pd.DataFrame:
    dfs = [pd.DataFrame(data_dict) for data_dict in data_dicts]
    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    return df


def query_videos_list(
    video_ids: list, youtube: googleapiclient.discovery.Resource
) -> list[dict]:
    slices = get_slices_video_ids(video_ids)
    responses = list()
    for vid_slice in slices:
        subset_video_ids = video_ids[vid_slice]
        response = get_video_data(subset_video_ids, youtube)
        responses.append(response)
    return responses


def get_slices_video_ids(video_ids: list[str]) -> list[slice]:
    """
    Get slices for looping over list of video ids.
    If len(video_ids) % 50 == 0 we should not add an extra part
    """
    num_videos = len(video_ids)
    num_parts = num_videos // 50 + 1 * (
        num_videos % 50 != 0
    )  # API limits 50 video ids pr. request
    slices = [slice(i * 50, (i + 1) * 50) for i in range(num_parts)]
    return slices


def get_video_data(vidid, youtube):
    """
    Queries the API for video data.
    """
    request = youtube.videos().list(
        part="snippet, statistics, ContentDetails", id=vidid
    )
    response = request.execute()
    return response


def get_region_restrictions(items):
    """Extracts the region code from an item if it exists else it returns 0"""
    region_restritions = list()
    for item in items:
        try:
            region_restritions.append(
                item["contentDetails"]["regionRestriction"]["blocked"]
            )
        except KeyError:
            region_restritions.append(["not-blocked"])
    return region_restritions


def get_data_videos(video_responses):
    if isinstance(video_responses, list):  # Vidoes queried individually
        items = [response["items"][0] for response in video_responses]
    else:
        items = video_responses["items"]
    # Extract values
    ids = [item["id"] for item in items]
    durations = [item["contentDetails"]["duration"] for item in items]
    dimensions = [item["contentDetails"]["dimension"] for item in items]
    region_restrictions = get_region_restrictions(items)
    view_counts = [item["statistics"]["viewCount"] for item in items]
    like_counts = [item["statistics"]["likeCount"] for item in items]
    favorite_counts = [item["statistics"]["favoriteCount"] for item in items]
    comment_counts = [item["statistics"].get("commentCount", np.nan) for item in items]
    # Return dictionary with extracted data
    return {
        "video_ids": ids,
        "durations": durations,
        "dimensions": dimensions,
        "region_restrictions": region_restrictions,
        "view_counts": view_counts,
        "like_counts": like_counts,
        "favorite_counts": favorite_counts,
        "comment_counts": comment_counts,
    }


def merge_playlist_and_videos(df_videos, df_playlist):
    df_m = df_playlist.merge(df_videos, how="left", on="video_ids")
    return df_m


def comments_pager(vidid, youtube, page_token=None):
    request = youtube.commentThreads().list(
        part="snippet", maxResults=100, videoId=vidid, pageToken=page_token
    )
    response = request.execute()
    return response


def query_single_comment(comment_id, youtube):
    request = youtube.comments().list(
        part="snippet", id=comment_id, 
    )
    response = request.execute()
    return response
    

@dataclass
class DataClassPager:
    """Storage of data for pager"""

    responses: list[dict]
    next_page_token: Union[None, str] = None
    all_items_collected: bool = False
    num_requests: int = 0


def get_tmp_file(item_id):
    file = FP_DATA_TMP / f"temp-{item_id}.pkl"
    return file


def init_pager(item_id):
    """Uses checkpoint for pager data else creates new container for data"""
    file = get_tmp_file(item_id)
    if file.exists():
        logging.info(f"Checkpoint exists for {item_id}. Starting from checkpoint...")
        pager_data: DataClassPager = data_utils.load_pickle_file(file)
    else:
        pager_data = DataClassPager(responses=list())
    return pager_data


def query_all_items(
    item_id: str, pager_func: Callable, youtube: googleapiclient.discovery.Resource
) -> list[dict]:
    """Collects all items for given pager function"""
    logging.info(f"Querying all items with func {pager_func.__name__}")
    pager_data = init_pager(item_id)
    while not pager_data.all_items_collected:
        try:
            response = pager_func(
                item_id, youtube, page_token=pager_data.next_page_token
            )
        except HttpError as e:
            logging.info(f"Error caught when paging for {item_id}")
            check_quota_exceeded_tmp(e, item_id, pager_data)
        pager_data.responses.append(response)
        pager_data.num_requests += 1
        if "nextPageToken" in response:
            pager_data.next_page_token = response["nextPageToken"]
        else:
            pager_data.all_items_collected = True
            logging.info(
                f"All items collected. Total #requests: {pager_data.num_requests}"
            )
            save_tmp_data(item_id, pager_data)  # Save tmp to avoid recollecting
    return pager_data.responses


def check_quota_exceeded_tmp(e: HttpError, item_id: str, data):
    """
    Check if error raised is because of QuotaExceeded and create tmp file
    to be used as a checkpoint
    """
    try:
        reason = unpack_http_error(e)
    except Exception:
        # Reraise the HTTP error if we can't unpack the reason
        raise e
    else:
        if reason == "quotaExceeded":
            # Save the data to create a checkpoint for next iteration when 
            # api key has been reset 
            logging.info(f"Quota exceed while querying items for {item_id}")
            save_tmp_data(item_id, data)
        # Reraise up to the decorator
        raise e


def save_tmp_data(item_id, data):
    file = get_tmp_file(item_id)
    data_utils.save_pickle(file, data)
    logging.info(f"Saved data for item id `{item_id}` to temp file {str(file)}")


def remove_tmp_data(item_id):
    """Remove tmp file if it exists"""
    file = get_tmp_file(item_id)
    if file.exists():
        file.unlink()
        logging.info(f"Removed temporary file {str(file)}")


def save_try_author(comment_snippet):
    try:
        return comment_snippet["authorChannelId"]["value"]
    except KeyError:
        logging.info(f"Caught keyerror for comment f{comment_snippet}")
        return np.nan


def extract_data_comment(comment: dict) -> dict:
    comment_snippet = comment["snippet"]
    data_top_level = {
        "comment_id": comment["id"],
        "text": comment_snippet["textDisplay"],
        "text_orig": comment_snippet["textOriginal"],
        "author": comment_snippet["authorDisplayName"],
        "author_profile_img": comment_snippet["authorProfileImageUrl"],
        "author_channel_url": comment_snippet["authorChannelUrl"],
        "author_channel_id": save_try_author(comment_snippet),
        "like_count": comment_snippet["likeCount"],
        "publish_date": comment_snippet["publishedAt"],
        "update_date": comment_snippet["updatedAt"],
    }
    return data_top_level


def collect_comments_in_thread(item, thread_id):
    comments = [
        extract_data_comment(comment) for comment in item["replies"]["comments"]
    ]
    for comment in comments:
        comment["thread_id"] = thread_id
    return comments


def get_multiple_comment_thread_data(
    responses_comment_thread: list[dict],
) -> list[dict]:
    data_thread = list()
    for response_comment_thread in responses_comment_thread:
        data_thread.extend(get_comment_thread_data(response_comment_thread))
    return data_thread


def get_comment_thread_data(response_comment_thread: dict) -> list[dict]:
    items = response_comment_thread["items"]
    data_thread = list()
    for item in items:
        # Thread stats
        thread_id = item["id"]
        snippet = item["snippet"]
        # Top level comment stats
        top_level_comment = snippet["topLevelComment"]
        data_top_level = extract_data_comment(top_level_comment)
        data_top_level["video_id"] = snippet["videoId"]
        data_top_level["total_reply_count"] = snippet["totalReplyCount"]
        data_top_level["thread_id"] = thread_id
        data_thread.append(data_top_level)
    return data_thread


def replies_pager(comment_id, youtube, page_token=None):
    request = youtube.comments().list(
        part="snippet", parentId=comment_id, pageToken=page_token, maxResults=50
    )
    response = request.execute()
    return response


def init_replies_dict(video_id) -> dict[str, list[dict]]:
    """Uses checkpoint for replies data else creates new dict for data"""
    id_replies = f"{video_id}-replies"
    file = get_tmp_file(id_replies)
    if file.exists():
        logging.info(
            f"Checkpoint exists for video_id `{video_id}`. "
            "Starting from checkpoint..."
        )
        comment_thread_responses: dict[str, list[dict]] = data_utils.load_pickle_file(
            file
        )
    else:
        comment_thread_responses = dict()
    return comment_thread_responses


def get_replies_comment_thread(
    video_id: str,
    data_thread: list[dict],
    youtube: googleapiclient.discovery.Resource,
    pager_func=replies_pager,
) -> dict[str, list[dict]]:
    """
    Returns dictionary with thread ids as key and list of response objects
    containing the replies in the thread
    """
    comment_thread_responses = init_replies_dict(video_id)
    for data_top_level in data_thread:
        if data_top_level["total_reply_count"] > 0:
            thread_id = data_top_level["thread_id"]
            if thread_id not in comment_thread_responses:
                # Only collect if we haven't collected it already
                try:
                    comment_thread_responses[thread_id] = query_all_items(
                        item_id=thread_id, pager_func=pager_func, youtube=youtube
                    )
                except HttpError as e:
                    # To distinguish it from a possible tmp file for toplevel
                    # comments
                    id_replies = f"{video_id}-replies"
                    check_quota_exceeded_tmp(e, id_replies, comment_thread_responses)
                else:
                    # Log info about request
                    num_responses = len(comment_thread_responses)
                    if num_responses % 20 == 0:
                        logging.info(
                            f"Collected replies for thread `{thread_id}`.\n"
                            f"Total amount of responses `{num_responses}` corresponding to"
                            f" ~{num_responses * 50} comments"
                        )
    return comment_thread_responses


def get_comment_reply_data(
    comment_thread_responses: dict[str, list[dict]]
) -> list[dict]:
    """
    Returns list of reply comments for all threads of the intput dictionary
    """
    data_replies: list[dict] = list()
    for thread_id in comment_thread_responses:
        responses = comment_thread_responses[thread_id]
        for response in responses:
            data_replies.extend(get_data_reply_comment(response, thread_id))
    return data_replies


def get_data_reply_comment(comment_response: dict, thread_id: str) -> list[dict]:
    """Returns list of comment data for given response object of given thread"""
    comments = [extract_data_comment(comment) for comment in comment_response["items"]]
    for comment in comments:
        comment["thread_id"] = thread_id
    return comments


def unpack_dict_of_lists(dict_of_lists: dict[str, list[dict]]) -> list[dict]:
    data = list()
    for list_values in dict_of_lists.values():
        data.extend(list_values)
    return data


def get_dfs_dicts(*args: list[dict]):
    return [pd.DataFrame(arg) for arg in args]
