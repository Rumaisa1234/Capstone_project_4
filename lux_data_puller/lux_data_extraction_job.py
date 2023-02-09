import logging
import os

import requests

logger = logging.getLogger()


rooms = ["kitchen", "bedroom", "bathroom", "living_room"]
url_prefix = os.environ.get("SENSORSMOCK_URL")


for room in rooms:
    response = requests.get(f"{url_prefix}/{room}").json()
    logger.info(f"Received luxmeter data: {response}")
