from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import loguru
from loguru import logger


def serialize(record: loguru.Record):
    subset = {
        "timestamp": record["time"].isoformat(),
        "msg": record["message"],
        "fields": {},
        "tags": [record["level"].name],
        "data": {
            "location": f'{record["name"]}:{record["function"]}:{record["line"]}',
        },
    }
    subset["fields"].update(record["extra"])
    return json.dumps(subset)


def json_sink(message: loguru.Message):
    serialized = serialize(message.record)
    print(serialized, file=sys.stderr)  # noqa: T201


def setup_logging(json: bool = False) -> None:
    """
    This function should only be called once
    """
    # Remove default sink
    if json:
        logger.remove()
        logger.add(json_sink)

    logger.configure(extra={"system": "", "environment": "", "deployment_id": ""})
