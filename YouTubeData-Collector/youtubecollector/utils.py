import logging
from youtubecollector import FP_LOGS
import datetime


def set_logger(prefix="", suffix=None):
    if not suffix:
        suffix = datetime.datetime.now().strftime("%y%m%d_%H%M")
    logfile_name = FP_LOGS / f"log-{prefix}-{suffix}.log"
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.DEBUG, filename=str(logfile_name))
