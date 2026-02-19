FROM apache/airflow:2.7.1

USER root
# ติดตั้ง Java (จำเป็นสำหรับ PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow
# ติดตั้ง PySpark และไลบรารีที่จำเป็น
RUN pip install pyspark==3.5.0