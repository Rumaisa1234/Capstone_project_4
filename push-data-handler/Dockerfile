FROM python:3.10-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
<<<<<<< HEAD:push-data-handler/Dockerfile



COPY push-data-handler.py push-data-handler.py

=======
COPY postapi.py postapi.py
>>>>>>> fc6ed09b1c0a5ed3fd46b1ba70292b0ef88ce16a:sensors/post/Dockerfile
ENV PYTHONPATH /app
CMD [ "python3", "push-data-handler.py"]
