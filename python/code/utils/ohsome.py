import datetime
from typing import Dict

import requests

from python.code.utils.definitions import OHSOME_API, logger


def query(request: Dict, bpolys: str, properties: str = None) -> Dict:
    """Query ohsome API endpoint with filter."""
    url = OHSOME_API + request["endpoint"]
    if properties is not None:
        data = {"bpolys": bpolys, "filter": request["filter"], "properties": properties}
    else:
        data = {"bpolys": bpolys, "filter": request["filter"]}
    logger.info("Query ohsome API.")
    logger.info("Query URL: " + url)
    logger.info("Query Filter: " + request["filter"])

    response = requests.post(url, data=data)

    if response.status_code == 200:
        logger.info("Query successful!")
    elif response.status_code == 404:
        logger.info("Query failed!")
    else:
        logger.info(response.status_code)
    return response.json()


def get_latest_ohsome_timestamp():
    """Get unix timestamp of ohsome from ohsome api."""
    url = "https://api.ohsome.org/v1/metadata"
    r = requests.get(url)
    timestamp_str = str(r.json()["extractRegion"]["temporalExtent"]["toTimestamp"])
    timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%MZ")
    return timestamp


