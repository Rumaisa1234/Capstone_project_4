FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY dbconnector.py dbconnector.py
RUN apt-get update && apt-get install -y procps  

CMD ["python3", "dbconnector.py"]

