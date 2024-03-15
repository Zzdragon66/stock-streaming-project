FROM python:3.11.8

WORKDIR /stock/

COPY ./python_requirements.txt .
RUN pip3 install --no-cache-dir -r python_requirements.txt

COPY ./lstm ./lstm 
COPY ./data_preprocess.py ./data_preprocess.py 
COPY ./database.py ./database.py
COPY ./kafka.py ./kafka.py
COPY ./stock_prediction_main.py ./stock_prediction_main.py
COPY ./scalar.save ./scalar.save