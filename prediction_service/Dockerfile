FROM python:3.8-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
COPY prediction.py prediction.py
COPY model.pkl model.pkl
RUN pip3 install -r requirements.txt
RUN apt-get update && apt-get install -y procps
CMD ["python3", "prediction.py"]

