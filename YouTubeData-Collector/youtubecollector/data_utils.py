import datetime
from collections import OrderedDict
from pathlib import Path
import pickle
import logging

import pandas as pd

from youtubecollector import FP_DATA_RAW, FP_DATA, FP_DATA_PROC, ytcollector


def load_data(fname: str) -> pd.DataFrame:
    """
    The old file with channels was `overview.xlsx`.
    The new is `channels.xlsx`
    """
    df = pd.read_excel(FP_DATA / fname, engine='openpyxl')
    return df


def get_channels_to_collect(df_overview: pd.DataFrame) -> dict[str, str]:
    df = (
        df_overview
        .assign(channel=lambda df: df.channel.str.lower().str.replace(" ", "-"))
    )
    channels_to_collect = OrderedDict(zip(df.channel, df.channel_id))
    return channels_to_collect


def save_df(df, name, folder):
    folder = FP_DATA_PROC / folder
    check_folder(folder)
    fname = get_name(name, suffix="gzip.parquet")
    df.to_parquet(folder / fname)
    logging.info(f"{fname} saved to {str(FP_DATA_RAW)}")


def save_response(response, name, folder):
    file = get_file_response(name, folder)
    save_pickle(file, response)
    logging.info(f"{name} saved to {str(file.parent)}")


def save_response_comments(
    responses: OrderedDict[str, list[dict]], name: str, folder: str
):
    file = get_file_response(name, folder)

    # Load previously constructed dict or create new
    response_comments = check_comments_file_exists(file)

    # Fill dictionary with thread id
    for video_id in responses:
        if video_id not in response_comments:  # Only insert new collected
            response = responses[video_id]
            response_comments[video_id] = response
    logging.info(f"Number of videos in saved response: {len(response_comments)}")

    # Save again
    save_pickle(file, response_comments)


def save_videos_with_disabled_comments(video_ids: list, name: str, folder: str) -> None:
    file = get_file_response(name, folder)
    save_pickle(file, video_ids)


def save_pickle(file, data):
    with open(file, "wb") as f:
        pickle.dump(data, f)


def get_file_response(name: str, folder: str) -> Path:
    folder = FP_DATA_RAW / folder
    check_folder(folder)
    name = get_name(name, suffix="pkl")
    file = folder / name
    return file


def get_current_time():
    return datetime.datetime.now().strftime('%F %T')


def get_name(name, suffix):
    date = datetime.datetime.now().strftime("%F")
    fname = f"{name}-{date}.{suffix}"
    return fname


def check_folder(folder: Path) -> None:
    if not folder.is_dir():
        Path.mkdir(folder)


def check_comments_file_exists(file: Path) -> OrderedDict[str, list[dict]]:
    if file.exists():
        with open(file, "rb") as f:
            response: OrderedDict[str, list[dict]] = pickle.load(f)
        logging.info(f"File already exists. Appending data to existing file..")
    else:
        response = OrderedDict()
    return response


def get_reply_count_comparison(df_replies, df_threads):
    gp1 = (
        df_replies.groupby("thread_id")
        .comment_id.count()
        .to_frame("actual_replies_api")
    )
    gp2 = df_threads.set_index("thread_id")[["total_reply_count"]].query(
        "total_reply_count > 0"
    )
    reply_count_comparison = pd.concat((gp1, gp2), axis=1)
    return reply_count_comparison


def query_1_month_df(df):
    mask = (
        df.upload_dates.apply(pd.to_datetime)
        .apply(ytcollector.is_uploaded_before_war)
        .astype(bool)
    )
    df = df[~mask].copy()
    return df


def load_pickle_file(file):
    with open(file, "rb") as f:
        loaded_file = pickle.load(f)
    return loaded_file

