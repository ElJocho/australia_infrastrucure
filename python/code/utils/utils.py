import os
import json
from python.code.utils.definitions import DATA_PATH, logger


def to_json(filename, key, value):
    """Drop country results."""
    path = os.path.join(DATA_PATH, filename)
    try:
        with open(path) as file:
            data = json.load(file)
    except FileNotFoundError:
        logger.info(
            "File {} not existing. Creating a new one.".format(
                filename
            )
        )
        data = {}

    data[key] = value

    with open(path, "w") as f:
        json.dump(data, f)
