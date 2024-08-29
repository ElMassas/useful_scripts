import json

from jsonschema import exceptions, validate
from loguru import logger


def validate_json(json_data: dict, SCHEMA: json) -> bool:
    try:
        validate(instance=json_data, schema=SCHEMA)
        return True
    except exceptions.ValidationError as e:
        error_path = " -> ".join([str(x) for x in e.path])
        error_message = f"Validation error at '{error_path}': {e.message}"
        logger.warning(
            f"Message is not valid with error: {error_message} for data: {json_data}"
        )
        return False
