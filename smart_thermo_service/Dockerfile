FROM python:3.10-slim-buster

WORKDIR /app
RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY smart_thermo_service.py smart_thermo_service.py 
RUN pip install requests
ENV PYTHONPATH /app

CMD [ "python3", "smart_thermo_service.py"]
