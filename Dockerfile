FROM apache/airflow:2.5.1
COPY requirements.txt /tmp/
RUN pip install --user --upgrade pip
RUN pip install -r /tmp/requirements.txt