FROM python:3.10-slim-buster
RUN apt-get update && apt-get -y install cron
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY lux_data_extraction_job.py lux_data_extraction_job.py
ENV PYTHONPATH /app
CMD ["python3", "lux_data_extraction_job.py"]
