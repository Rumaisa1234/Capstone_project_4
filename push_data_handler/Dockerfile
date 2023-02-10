FROM python:3.10-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY push_data_handler.py push_data_handler.py
ENV PYTHONPATH /app
CMD [ "python3", "push_data_handler.py"]
