import traceback
from typing import Any

from loguru import logger
from psycopg2 import connect


def db_connect(
    host: str, database: str, user: str, password: str, port: str = "5432"
) -> Any | None:
    try:
        rds_conn = connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            options="-c statement_timeout=3s",
        )
        logger.info(
            "[POSTGRES] Connected to DB",
            fields={"DB": "{database}", "log_level": 4},
        )
        return rds_conn
    except Exception as err:
        logger.critical(
            "[POSTGRES]Failed to connect to the database!",
            fields={
                "dsn": f"host={host} port={port} database={database} user={user} password=<obfuscated>",  # noqa
                "exception_class": str(err.__class__),
                "traceback": f"{traceback.format_exc()}",
                "exception": str(err),
            },
        )
        raise err


def sql_error_handler(err: Exception, query: str) -> None:
    logger.error(
        "[POSTGRES] Error executing query",
        fields={
            "class": f"{str(err.__class__.__name__)}",
            "traceback": f"{traceback.format_exc()}",
            "str": f"{str(err)}",
            "query": query,
        },
    )
    raise
