import logging
import json
from threading import Lock

from vda.common.constant import Constant


def loginit():
    logger = logging.getLogger(Constant.PACKAGE_NAME)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(Constant.LOGGING_FMT)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)


def create_db_args(parser):
    db_uri_group = parser.add_mutually_exclusive_group()
    db_uri_group.add_argument(
        "--db-uri",
        default=Constant.DEFAULT_DB_URI,
        help="pass db uri from this parameter",
    )

    db_uri_group.add_argument(
        "--db-uri-file",
        help="pass db uri from a file",
    )

    db_uri_group.add_argument(
        "--db-uri-stdin",
        action='store_true',
        help="pass db uri from stdin",
    )

    parser.add_argument(
        "--db-kwargs",
        default="{}",
        help="sqlalchemy create_engine kwargs",
    )


def get_db_args(args):
    if args.db_uri_stdin:
        db_uri = input("db uri: ")
    elif args.db_uri_file:
        with open(args.db_uri_file) as f:
            db_uri = f.read().strip()
    else:
        db_uri = args.db_uri

    db_kwargs = json.loads(args.db_kwargs)
    return db_uri, db_kwargs


class AtomicCounter:

    def __init__(self):
        self.value = 0
        self.lock = Lock()

    def inc(self):
        with self.lock:
            self.value += 1

    def add(self, val):
        with self.lock:
            self.value += val

    def get(self):
        return self.value


class Histogram:

    def __init__(self, min_val, width, steps):
        self.min_val = min_val
        self.max_val = min_val + width * steps
        self.width = width
        self.h = [AtomicCounter() for i in range(steps+2)]

    def update(self, val):
        if val < self.min_val:
            idx = 0
        elif val >= self.max_val:
            idx = -1
        else:
            idx = (val - self.min_val) // self.width
            idx += 1
        self.h[idx].inc()

    def get(self):
        return [c.get() for c in self.h]
