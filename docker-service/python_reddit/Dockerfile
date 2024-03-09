FROM python:3.11.8

WORKDIR /stock/

COPY ./reddit-producer.py ./reddit-producer.py
COPY ./python_requirements.txt ./python_requirements.txt

RUN pip3 install --no-cache-dir -r python_requirements.txt