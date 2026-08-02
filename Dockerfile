FROM apache/spark:3.5.1

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY postgresql-42.7.3.jar /opt/spark/jars/

USER spark