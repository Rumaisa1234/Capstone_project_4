
FROM python:3.8-slim-buster
WORKDIR /app
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    pip install pyspark
COPY requirements.txt requirements.txt
COPY Transformation.py Transformation.py
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y procps  
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON python3

CMD ["spark-submit", "Transformation.py"]

