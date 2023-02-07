import logging

import requests

logger = logging.getLogger()


rooms = ["kitchen", "bedroom", "bathroom", "living_room"]
url_prefix = "http://sensorsmock:3000/api/luxmeter"


for room in rooms:
    response = requests.get(f"{url_prefix}/{room}").json()
    print(response)
