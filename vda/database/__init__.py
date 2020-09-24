import argparse

from sqlalchemy import create_engine

from vda.common.utils import loginit, create_db_args, get_db_args
from vda.common.modules import Base


def main():
    parser = argparse.ArgumentParser(
        prog="vda_db",
        add_help=True,
    )
    parser.add_argument(
        "--action",
        choices=["create", "drop"],
        help="create all tables or drop all tables",
    )

    create_db_args(parser)

    loginit()

    args = parser.parse_args()
    db_uri, db_kwargs = get_db_args(args)
    engine = create_engine(db_uri, **db_kwargs)
    if args.action == "create":
        Base.metadata.create_all(engine)
    elif args.action == "drop":
        Base.metadata.drop_all(engine)
    else:
        raise Exception(f"unknown action: {args.action}")
