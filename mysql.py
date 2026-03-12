#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pymysql",
#     "python-dotenv",
# ]
# ///

import argparse
import json
import logging
import os

import dotenv
import pymysql

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DATABASES = {
    "replica": {
        "user": "DB_REPLICA_MYSQL_USER",
        "password": "DB_REPLICA_MYSQL_PASSWORD",
        "host": "DB_REPLICA_MYSQL_HOST",
        "port": "DB_REPLICA_MYSQL_PORT",
        "database": "DB_REPLICA_MYSQL_DATABASE",
    },
    "egflstats": {
        "user": "DB_EGFLSTATS_MYSQL_USER",
        "password": "DB_EGFLSTATS_MYSQL_PASSWORD",
        "host": "DB_EGFLSTATS_MYSQL_HOST",
        "port": "DB_EGFLSTATS_MYSQL_PORT",
        "database": "DB_EGFLSTATS_MYSQL_DATABASE",
    },
}


def get_connection(db_name):
    env_vars = DATABASES[db_name]
    log.info("Connecting to %s (%s)", db_name, os.environ[env_vars["host"]])
    return pymysql.connect(
        user=os.environ[env_vars["user"]],
        password=os.environ[env_vars["password"]],
        host=os.environ[env_vars["host"]],
        port=int(os.environ[env_vars["port"]]),
        database=os.environ[env_vars["database"]],
        cursorclass=pymysql.cursors.DictCursor,
    )


def main():
    parser = argparse.ArgumentParser(description="Run MySQL queries against egflstats or replica databases")
    parser.add_argument("query", help="SQL query to execute")
    parser.add_argument("-d", "--database", choices=list(DATABASES), default="replica", help="Database to query (default: replica)")
    args = parser.parse_args()

    dotenv.load_dotenv()

    log.info("Database: %s", args.database)
    log.info("Query: %s", args.query)

    conn = get_connection(args.database)
    cursor = conn.cursor()
    cursor.execute(args.query)
    rows = cursor.fetchall()
    log.info("Rows returned: %d", len(rows))

    for row in rows:
        print(json.dumps(row, default=str))

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
