import requests

url_1 = "http://sensorsmock:3000/api/luxmeter/kitchen"
url_2 = "http://sensorsmock:3000/api/luxmeter/bedroom"
url_3 = "http://sensorsmock:3000/api/luxmeter/bathroom"
url_4 = "http://sensorsmock:3000/api/luxmeter/living_room"

x_1 = requests.get(url_1)
x_2 = requests.get(url_2)
x_3 = requests.get(url_3)
x_4 = requests.get(url_4)
print(x_1.text)
print(x_2.text)
print(x_3.text)
print(x_4.text)
