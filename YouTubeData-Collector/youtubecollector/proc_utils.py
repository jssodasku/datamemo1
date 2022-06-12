import re
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from youtubecollector import FP_DATA_PROC, FP_DATA_RAW
from youtubecollector import data_utils, ytcollector


IGNORE_CHANNELS = []  # Ignore for now
CHANNELS = list((f.name for f in FP_DATA_RAW.glob('*') 
                 if f.is_dir() and f not in IGNORE_CHANNELS))


def get_df(
    name: str, get_dfs_func: Callable[[str], dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    dfs = get_dfs_func(name)
    dfs = (df.assign(channel=channel) for channel, df in dfs.items())
    df = pd.concat(dfs, axis=0).reset_index(drop=True)
    return df


def get_dfs(name: str) -> dict[str, pd.DataFrame]:
    file_dict = get_file_dict(name, proc=True)
    dfs = {channel: pd.read_parquet(df) for channel, df in file_dict.items()}
    return dfs


def get_comment_data(name: str) -> dict[str, pd.DataFrame]:
    # Get file paths from raw data 
    collected_comments: dict[str, Path] = get_file_dict(name, proc=False)
    data_comments = {
        channel: pd.DataFrame(
            ytcollector.unpack_dict_of_lists(
                data_utils.load_pickle_file(collected_comments[channel])
            )
        )
        for channel in collected_comments
    }
    return data_comments


def get_file_dict(name: str, proc: bool = False) -> dict[str, Path]:
    if proc:
        main_folder = FP_DATA_PROC
    else:
        main_folder = FP_DATA_RAW
    file_dict = {
        channel: get_file(main_folder / channel, name)
        for channel in CHANNELS
        if channel not in IGNORE_CHANNELS
    }
    return file_dict


def get_file(folder: Path, name: str) -> Path:
    files = list(folder.glob(f"*{name}*"))
    if (len_files := len(files)) == 1:
        (file,) = files
    elif len_files > 1:
        file = select_latest_file(files)
    else:
        raise AssertionError(f"{len_files} files with `{name}` in {folder}")
    return file


def select_latest_file(files: list[Path]) -> Path:
    files = sorted(files, key=get_file_date, reverse=True)
    return files[0]


RE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def get_file_date(file: Path) -> pd.Timestamp:
    match = RE_DATE.search(file.name)
    if not match:
        raise AssertionError("No match")
    ts = pd.Timestamp(match.group(0))
    return ts


def get_df_comments(
    df_toplevel: pd.DataFrame, df_replies: pd.DataFrame
) -> pd.DataFrame:
    mapping = get_mapping_threadid_to_videoid(df_toplevel)
    df_replies = add_missing_columns_replies(df_replies, mapping)
    df_comments = concat_replies_onto_toplevel(df_replies, df_toplevel)
    return df_comments


def get_mapping_threadid_to_videoid(df_toplevel: pd.DataFrame) -> dict:
    """Map thread id to video id for the replies dataframe"""
    cols = ["thread_id", "video_id"]
    mapping = (
        df_toplevel[cols].drop_duplicates().set_index("thread_id")["video_id"].to_dict()
    )
    return mapping


def add_missing_columns_replies(
    df_replies: pd.DataFrame, mapping: dict
) -> pd.DataFrame:
    df_replies["total_reply_count"] = np.nan
    df_replies["video_id"] = df_replies["thread_id"].map(mapping)
    return df_replies


def concat_replies_onto_toplevel(
    df_replies: pd.DataFrame, df_toplevel: pd.DataFrame
) -> pd.DataFrame:
    return pd.concat(
        (df_toplevel.assign(toplevel=True), df_replies.assign(toplevel=False)), axis=0
    ).reset_index(drop=True)


def load_proc_data(name):
    folder = FP_DATA_PROC / "all"
    file = get_file(folder, name)
    df = pd.read_parquet(file)
    return df
