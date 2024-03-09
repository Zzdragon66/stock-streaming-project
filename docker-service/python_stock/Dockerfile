FROM python:3.11.8

WORKDIR /stock/

COPY ./stock_data_generation .
COPY ./python_requirements.txt .

RUN pip3 install --no-cache-dir -r python_requirements.txt

#docker run zzdragon/stock-generator python3 stock_generation_script.py arg1 arg2