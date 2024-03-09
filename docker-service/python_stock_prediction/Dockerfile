FROM python:3.11.8

WORKDIR /stock/

COPY ./lstm_model .
COPY ./python_requirements.txt .

RUN pip3 install --no-cache-dir -r python_requirements.txt